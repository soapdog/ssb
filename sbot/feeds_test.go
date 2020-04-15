// SPDX-License-Identifier: MIT

package sbot

import (
	"context"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"go.cryptoscope.co/margaret"
	"go.cryptoscope.co/ssb"
	"go.cryptoscope.co/ssb/internal/leakcheck"
	"go.cryptoscope.co/ssb/internal/testutils"
)

func TestFeedsOneByOne(t *testing.T) {
	defer leakcheck.Check(t)
	r := require.New(t)
	a := assert.New(t)
	ctx, cancel := ShutdownContext(context.Background())

	os.RemoveAll("testrun")

	appKey := make([]byte, 32)
	rand.Read(appKey)
	hmacKey := make([]byte, 32)
	rand.Read(hmacKey)

	botgroup, ctx := errgroup.WithContext(ctx)

	mainLog := testutils.NewRelativeTimeLogger(nil)

	ali, err := New(
		WithAppKey(appKey),
		WithHMACSigning(hmacKey),
		WithContext(ctx),
		WithInfo(log.With(mainLog, "unit", "ali")),
		// WithPostSecureConnWrapper(func(conn net.Conn) (net.Conn, error) {
		// 	return debug.WrapConn(log.With(aliLog, "who", "a"), conn), nil
		// }),
		WithRepoPath(filepath.Join("testrun", t.Name(), "ali")),
		WithListenAddr(":0"),
		// LateOption(MountPlugin(&bytype.Plugin{}, plugins2.AuthMaster)),
	)
	r.NoError(err)

	botgroup.Go(func() error {
		err := ali.Network.Serve(ctx)
		if err != nil {
			level.Warn(mainLog).Log("event", "ali serve exited", "err", err)
		}
		if errors.Cause(err) == ssb.ErrShuttingDown {
			return nil
		}
		return err
	})

	bob, err := New(
		WithAppKey(appKey),
		WithHMACSigning(hmacKey),
		WithContext(ctx),
		WithInfo(log.With(mainLog, "unit", "bob")),
		// WithConnWrapper(func(conn net.Conn) (net.Conn, error) {
		// 	return debug.WrapConn(bobLog, conn), nil
		// }),
		WithRepoPath(filepath.Join("testrun", t.Name(), "bob")),
		WithListenAddr(":0"),
		// LateOption(MountPlugin(&bytype.Plugin{}, plugins2.AuthMaster)),
	)
	r.NoError(err)

	botgroup.Go(func() error {
		err := bob.Network.Serve(ctx)
		if err != nil {
			level.Warn(mainLog).Log("event", "bob serve exited", "err", err)
		}
		if errors.Cause(err) == ssb.ErrShuttingDown {
			return nil
		}
		return err
	})

	seq, err := ali.PublishLog.Append(ssb.Contact{
		Type:      "contact",
		Following: true,
		Contact:   bob.KeyPair.Id,
	})
	r.NoError(err)
	r.Equal(margaret.BaseSeq(0), seq)

	seq, err = bob.PublishLog.Append(ssb.Contact{
		Type:      "contact",
		Following: true,
		Contact:   ali.KeyPair.Id,
	})
	r.NoError(err)

	g, err := bob.GraphBuilder.Build()
	r.NoError(err)
	time.Sleep(250 * time.Millisecond)
	r.True(g.Follows(bob.KeyPair.Id, ali.KeyPair.Id))

	uf, ok := bob.GetMultiLog("userFeeds")
	r.True(ok)
	alisLog, err := uf.Get(ali.KeyPair.Id.StoredAddr())
	r.NoError(err)

	for i := 0; i < 50; i++ {
		t.Logf("runniung connect %d", i)
		err = bob.Network.Connect(ctx, ali.Network.GetListenAddr())
		r.NoError(err)
		time.Sleep(100 * time.Millisecond)
		bob.Network.GetConnTracker().CloseAll()

		_, err := ali.PublishLog.Append(map[string]interface{}{
			"test": i,
		})
		r.NoError(err)

		seqv, err := alisLog.Seq().Value()
		r.NoError(err)
		a.Equal(margaret.BaseSeq(i), seqv, "check run %d", i)
	}

	r.NoError(ali.FSCK(), "fsck bot A failed")
	r.NoError(bob.FSCK(), "fsck bot B failed")

	cancel()
	ali.Shutdown()
	bob.Shutdown()

	r.NoError(ali.Close())
	r.NoError(bob.Close())

	r.NoError(botgroup.Wait())
}
