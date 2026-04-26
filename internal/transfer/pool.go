package transfer

import (
	"context"
	"sync"
)

// job is a unit of transfer work.
type job struct {
	ctx      context.Context
	fn       func() error
	resultCh chan error
}

// pool runs a bounded number of concurrent workers.
type pool struct {
	jobCh chan job
	wg    sync.WaitGroup
}

func newPool(maxWorkers int) *pool {
	p := &pool{jobCh: make(chan job, maxWorkers*4)}
	for i := 0; i < maxWorkers; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for j := range p.jobCh {
				j.resultCh <- j.fn()
			}
		}()
	}
	return p
}

// submit enqueues a job and returns a channel that receives the result.
func (p *pool) submit(ctx context.Context, fn func() error) <-chan error {
	ch := make(chan error, 1)
	p.jobCh <- job{ctx: ctx, fn: fn, resultCh: ch}
	return ch
}

func (p *pool) close() {
	close(p.jobCh)
	p.wg.Wait()
}
