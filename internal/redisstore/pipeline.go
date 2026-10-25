package redisstore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/redis/go-redis/v9"
)

// ErrClosed is returned for checks sent to a closed Pipeline.
var ErrClosed = errors.New("redisstore: pipeline closed")

// PipelineClient is what a Pipeline needs from a Redis client.
// *redis.Client, *redis.ClusterClient and *redis.Ring all satisfy it.
type PipelineClient interface {
	redis.Scripter
	Pipeline() redis.Pipeliner
}

// PipelineOptions configures a Pipeline. Zero values select the defaults.
type PipelineOptions struct {
	// Senders is how many pipelines may be in flight at once, each on its
	// own connection. Defaults to 4.
	Senders int
	// MaxBatch caps the commands in one pipeline. Defaults to 128.
	MaxBatch int
}

// Pipeline coalesces the script calls of concurrent checks into pipelined
// round trips. It is a redis.Scripter, so any limiter in this package can
// run on it unchanged:
//
//	p := redisstore.NewPipeline(rdb, redisstore.PipelineOptions{})
//	defer p.Close()
//	lim := redisstore.NewGCRA(p, "")
//
// There is no timer and no linger. A call is handed straight to an idle
// sender, so a lone check still costs one round trip and no added latency.
// Calls that arrive while every sender is busy wait for the next free one,
// which takes all of them at once, up to MaxBatch: the busier the service,
// the larger the batches and the fewer the round trips per check.
//
// Every script still runs atomically on its own; pipelining only removes
// network round trips and system calls, on the client and on Redis.
type Pipeline struct {
	client   PipelineClient
	maxBatch int
	calls    chan *call // unbuffered: a send succeeds only when a sender takes it

	ctx    context.Context // ends on Close, aborting in-flight pipelines
	cancel context.CancelFunc
	closed chan struct{}
	once   sync.Once
	wg     sync.WaitGroup

	batches  atomic.Uint64
	commands atomic.Uint64
}

type call struct {
	cmd  *redis.Cmd
	done chan struct{}
}

var _ redis.Scripter = (*Pipeline)(nil)

// NewPipeline starts the senders of a Pipeline over client. Close stops them.
func NewPipeline(client PipelineClient, opts PipelineOptions) *Pipeline {
	if opts.Senders <= 0 {
		opts.Senders = 4
	}
	if opts.MaxBatch <= 0 {
		opts.MaxBatch = 128
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &Pipeline{
		client: client, maxBatch: opts.MaxBatch, calls: make(chan *call),
		ctx: ctx, cancel: cancel, closed: make(chan struct{}),
	}
	p.wg.Add(opts.Senders)
	for range opts.Senders {
		go p.send()
	}
	return p
}

// send takes one call, waiting as long as it must, then whatever else is
// already queued, and sends them as one pipeline.
func (p *Pipeline) send() {
	defer p.wg.Done()
	batch := make([]*call, 0, p.maxBatch)
	for {
		select {
		case c := <-p.calls:
			batch = append(batch, c)
		case <-p.closed:
			return
		}
	gather:
		for len(batch) < p.maxBatch {
			select {
			case c := <-p.calls:
				batch = append(batch, c)
			default:
				break gather
			}
		}

		pipe := p.client.Pipeline()
		for _, c := range batch {
			_ = pipe.Process(p.ctx, c.cmd) // queues the command; errors arrive with Exec
		}
		// Each command normally carries its own reply or error. When no
		// connection could be had at all (pool timeout, Close), go-redis
		// returns the error without setting it on the commands, so set it
		// on any command left with neither: a script reply is never nil.
		if _, err := pipe.Exec(p.ctx); err != nil {
			for _, c := range batch {
				if c.cmd.Err() == nil && c.cmd.Val() == nil {
					c.cmd.SetErr(err)
				}
			}
		}
		p.batches.Add(1)
		p.commands.Add(uint64(len(batch)))
		for i, c := range batch {
			close(c.done)
			batch[i] = nil
		}
		batch = batch[:0]
	}
}

// do sends cmd through a sender and waits for its reply. A caller whose
// context ends gets the context's error straight away; its command may
// still run, exactly as when a direct call times out after writing it.
func (p *Pipeline) do(ctx context.Context, cmd *redis.Cmd) *redis.Cmd {
	c := &call{cmd: cmd, done: make(chan struct{})}
	select {
	case p.calls <- c:
	case <-ctx.Done():
		return failed(ctx, ctx.Err())
	case <-p.closed:
		return failed(ctx, ErrClosed)
	}
	select {
	case <-c.done:
		return cmd
	case <-ctx.Done():
		// The sender still owns cmd and will write to it; hand back a
		// separate command instead.
		return failed(ctx, ctx.Err())
	}
}

func failed(ctx context.Context, err error) *redis.Cmd {
	cmd := redis.NewCmd(ctx)
	cmd.SetErr(err)
	return cmd
}

// eval builds the command exactly as go-redis does for its own Eval family.
func (p *Pipeline) eval(ctx context.Context, name, payload string, keys []string, args []any) *redis.Cmd {
	cmdArgs := make([]any, 0, 3+len(keys)+len(args))
	cmdArgs = append(cmdArgs, name, payload, len(keys))
	for _, k := range keys {
		cmdArgs = append(cmdArgs, k)
	}
	cmdArgs = append(cmdArgs, args...)
	cmd := redis.NewCmd(ctx, cmdArgs...)
	if len(keys) > 0 {
		cmd.SetFirstKeyPos(3) // lets a cluster client route by the first key
	}
	return p.do(ctx, cmd)
}

// Eval implements redis.Scripter.
func (p *Pipeline) Eval(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd {
	return p.eval(ctx, "eval", script, keys, args)
}

// EvalSha implements redis.Scripter.
func (p *Pipeline) EvalSha(ctx context.Context, sha1 string, keys []string, args ...any) *redis.Cmd {
	return p.eval(ctx, "evalsha", sha1, keys, args)
}

// EvalRO implements redis.Scripter.
func (p *Pipeline) EvalRO(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd {
	return p.eval(ctx, "eval_ro", script, keys, args)
}

// EvalShaRO implements redis.Scripter.
func (p *Pipeline) EvalShaRO(ctx context.Context, sha1 string, keys []string, args ...any) *redis.Cmd {
	return p.eval(ctx, "evalsha_ro", sha1, keys, args)
}

// ScriptExists implements redis.Scripter. It is rare and goes straight to
// the client.
func (p *Pipeline) ScriptExists(ctx context.Context, hashes ...string) *redis.BoolSliceCmd {
	return p.client.ScriptExists(ctx, hashes...)
}

// ScriptLoad implements redis.Scripter. It is rare and goes straight to the
// client.
func (p *Pipeline) ScriptLoad(ctx context.Context, script string) *redis.StringCmd {
	return p.client.ScriptLoad(ctx, script)
}

// PipelineStats counts what a Pipeline has sent. Commands/Batches is the
// average batch size: 1 when idle, growing with load.
type PipelineStats struct {
	Batches  uint64 // round trips
	Commands uint64 // script calls carried by them
}

// Stats returns the counts so far.
func (p *Pipeline) Stats() PipelineStats {
	return PipelineStats{Batches: p.batches.Load(), Commands: p.commands.Load()}
}

// Close stops accepting calls, aborts pipelines still in flight and waits
// for the senders to exit. It does not close the client. Calling it again
// does nothing.
func (p *Pipeline) Close() error {
	p.once.Do(func() {
		close(p.closed)
		p.cancel()
		p.wg.Wait()
	})
	return nil
}

func (s PipelineStats) String() string {
	return fmt.Sprintf("%d commands in %d round trips", s.Commands, s.Batches)
}
