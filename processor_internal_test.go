package streams

import (
	"context"
	"testing"

	"github.com/msales/streams/mocks"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestBranchProcessor_WithContext(t *testing.T) {
	ctx := mocks.NewContext(t)
	p := NewBranchProcessor([]Predicate{})

	p.WithContext(ctx)

	assert.Equal(t, ctx, p.(*BranchProcessor).ctx)
}

func TestBranchProcessor_Process(t *testing.T) {
	truePred := func(ctx context.Context, k, v interface{}) (bool, error) {
		return true, nil
	}
	falsePred := func(ctx context.Context, k, v interface{}) (bool, error) {
		return false, nil
	}
	ctx := mocks.NewContext(t)
	ctx.ExpectForwardToChild("test", "test", 0)
	p := NewBranchProcessor([]Predicate{truePred, falsePred})
	p.WithContext(ctx)

	err := p.Process(nil, "test", "test")

	assert.NoError(t, err)
	ctx.AssertExpectations()
}

func TestBranchProcessor_ProcessWithError(t *testing.T) {
	errPred := func(ctx context.Context, k, v interface{}) (bool, error) {
		return true, errors.New("test")
	}
	ctx := mocks.NewContext(t)
	p := NewBranchProcessor([]Predicate{errPred})
	p.WithContext(ctx)

	err := p.Process(nil, "test", "test")

	assert.Error(t, err)
}

func TestBranchProcessor_ProcessWithForwardError(t *testing.T) {
	pred := func(ctx context.Context, k, v interface{}) (bool, error) {
		return true, nil
	}
	ctx := mocks.NewContext(t)
	ctx.ExpectForwardToChild("test", "test", 0)
	ctx.ShouldError()
	p := NewBranchProcessor([]Predicate{pred})
	p.WithContext(ctx)

	err := p.Process(nil, "test", "test")

	assert.Error(t, err)
}

func TestBranchProcessor_Close(t *testing.T) {
	p := NewBranchProcessor([]Predicate{})

	err := p.Close()

	assert.NoError(t, err)
}

func TestFilterProcessor_WithContext(t *testing.T) {
	ctx := mocks.NewContext(t)
	p := NewFilterProcessor(nil)

	p.WithContext(ctx)

	assert.Equal(t, ctx, p.(*FilterProcessor).ctx)
}

func TestFilterProcessor_Process(t *testing.T) {
	pred := func(ctx context.Context, k, v interface{}) (bool, error) {
		if _, ok := k.(string); ok {
			return true, nil
		}

		return false, nil
	}
	ctx := mocks.NewContext(t)
	ctx.ExpectForward("test", "test")
	p := NewFilterProcessor(pred)
	p.WithContext(ctx)

	p.Process(nil, 1, 1)
	p.Process(nil, "test", "test")

	ctx.AssertExpectations()
}

func TestFilterProcessor_ProcessWithError(t *testing.T) {
	errPred := func(ctx context.Context, k, v interface{}) (bool, error) {
		return true, errors.New("test")
	}
	ctx := mocks.NewContext(t)
	p := NewFilterProcessor(errPred)
	p.WithContext(ctx)

	err := p.Process(nil, "test", "test")

	assert.Error(t, err)
}

func TestFilterProcessor_Close(t *testing.T) {
	p := NewFilterProcessor(nil)

	err := p.Close()

	assert.NoError(t, err)
}

func TestMapProcessor_WithContext(t *testing.T) {
	ctx := mocks.NewContext(t)
	p := NewMapProcessor(nil)

	p.WithContext(ctx)

	assert.Equal(t, ctx, p.(*MapProcessor).ctx)
}

func TestMapProcessor_Process(t *testing.T) {
	mapper := func(ctx context.Context, key, value interface{}) (context.Context, interface{}, interface{}, error) {
		return nil, 1, 1, nil
	}
	ctx := mocks.NewContext(t)
	ctx.ExpectForward(1, 1)
	p := NewMapProcessor(mapper)
	p.WithContext(ctx)

	p.Process(nil, "test", "test")

	ctx.AssertExpectations()
}

func TestMapProcessor_ProcessWithError(t *testing.T) {
	mapper := func(ctx context.Context, key, value interface{}) (context.Context, interface{}, interface{}, error) {
		return nil, nil, nil, errors.New("test")
	}
	ctx := mocks.NewContext(t)
	p := NewMapProcessor(mapper)
	p.WithContext(ctx)

	err := p.Process(nil, "test", "test")

	assert.Error(t, err)
}

func TestMapProcessor_Close(t *testing.T) {
	p := NewMapProcessor(nil)

	err := p.Close()

	assert.NoError(t, err)
}

func TestMergeProcessor_WithContext(t *testing.T) {
	ctx := mocks.NewContext(t)
	p := NewMergeProcessor()

	p.WithContext(ctx)

	assert.Equal(t, ctx, p.(*MergeProcessor).ctx)
}

func TestMergeProcessor_Process(t *testing.T) {
	ctx := mocks.NewContext(t)
	ctx.ExpectForward("test", "test")
	p := NewMergeProcessor()
	p.WithContext(ctx)

	p.Process(nil, "test", "test")

	ctx.AssertExpectations()
}

func TestMergeProcessor_Close(t *testing.T) {
	p := NewMergeProcessor()

	err := p.Close()

	assert.NoError(t, err)
}

func TestPrintProcessor_WithContext(t *testing.T) {
	ctx := mocks.NewContext(t)
	p := NewPrintProcessor()

	p.WithContext(ctx)

	assert.Equal(t, ctx, p.(*PrintProcessor).ctx)
}

func TestPrintProcessor_Process(t *testing.T) {
	ctx := mocks.NewContext(t)
	ctx.ExpectForward("test", "test")
	p := NewPrintProcessor()
	p.WithContext(ctx)

	p.Process(nil, "test", "test")

	ctx.AssertExpectations()
}

func TestPrintProcessor_Close(t *testing.T) {
	p := NewPrintProcessor()

	err := p.Close()

	assert.NoError(t, err)
}
