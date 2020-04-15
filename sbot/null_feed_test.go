package sbot

import (
	"context"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	kitlog "github.com/go-kit/kit/log"
	"github.com/stretchr/testify/require"
	"go.cryptoscope.co/margaret"
	"golang.org/x/sync/errgroup"

	"go.cryptoscope.co/ssb"
	"go.cryptoscope.co/ssb/indexes"
	"go.cryptoscope.co/ssb/internal/mutil"
	"go.cryptoscope.co/ssb/internal/testutils"
	"go.cryptoscope.co/ssb/repo"
)

func TestNullFeed(t *testing.T) {
	// defer leakcheck.Check(t)
	ctx, cancel := context.WithCancel(context.TODO())
	botgroup, ctx := errgroup.WithContext(ctx)
	logger := testutils.NewRelativeTimeLogger(nil)
	bs := newBotServer(ctx, logger)

	r := require.New(t)

	hk := make([]byte, 32)
	n, err := rand.Read(hk)
	r.Equal(32, n)

	tRepoPath := filepath.Join("testrun", t.Name())
	os.RemoveAll(tRepoPath)

	tRepo := repo.New(filepath.Join(tRepoPath, "main"))

	// make three new keypairs with nicknames
	n2kp := make(map[string]*ssb.KeyPair)

	kpArny, err := repo.NewKeyPair(tRepo, "arny", ssb.RefAlgoFeedSSB1)
	r.NoError(err)
	n2kp["arny"] = kpArny

	kpBert, err := repo.NewKeyPair(tRepo, "bert", ssb.RefAlgoFeedGabby)
	r.NoError(err)
	n2kp["bert"] = kpBert

	kps, err := repo.AllKeyPairs(tRepo)
	r.NoError(err)
	r.Len(kps, 2)

	// make the bot
	mainbot, err := New(
		WithInfo(kitlog.With(logger, "bot", "main")),
		WithRepoPath(filepath.Join(tRepoPath, "main")),
		WithHops(2),
		WithHMACSigning(hk),
		LateOption(MountSimpleIndex("get", indexes.OpenGet)),
		WithListenAddr(":0"),
	)
	r.NoError(err)
	botgroup.Go(bs.Serve(mainbot))

	// create some messages
	intros := []struct {
		as string      // nick name
		c  interface{} // content
	}{
		{"arny", ssb.NewContactFollow(kpBert.Id)},
		{"bert", ssb.NewContactFollow(kpArny.Id)},
		{"arny", map[string]interface{}{"hello": 123}},
		{"bert", map[string]interface{}{"world": 456}},
		{"bert", map[string]interface{}{"spew": true, "delete": "me"}},
	}

	for idx, intro := range intros {
		ref, err := mainbot.PublishAs(intro.as, intro.c)
		r.NoError(err, "publish %d failed", idx)
		r.NotNil(ref)
		msg, err := mainbot.Get(*ref)
		r.NoError(err)
		r.NotNil(msg)

		r.True(msg.Author().Equal(n2kp[intro.as].Id))
	}

	// assert helper
	checkLogSeq := func(l margaret.Log, seq int) {
		v, err := l.Seq().Value()
		r.NoError(err)
		r.EqualValues(seq, v.(margaret.Seq).Seq())
	}

	checkUserLogSeq := func(bot *Sbot, name string, seq int) {
		kp, has := n2kp[name]
		r.True(has, "%s not in map", name)

		uf, ok := bot.GetMultiLog("userFeeds")
		r.True(ok, "userFeeds mlog not present")

		l, err := uf.Get(kp.Id.StoredAddr())
		r.NoError(err)

		checkLogSeq(l, seq)
	}

	checkLogSeq(mainbot.RootLog, len(intros)-1) // got all the messages

	// check before drop
	checkUserLogSeq(mainbot, "arny", 1)
	checkUserLogSeq(mainbot, "bert", 2)

	err = mainbot.NullFeed(kpBert.Id)
	r.NoError(err, "null feed bert failed")

	checkUserLogSeq(mainbot, "arny", 1)
	checkUserLogSeq(mainbot, "bert", -1)

	// start bert and publish some new messages
	bertBot, err := New(
		WithKeyPair(kpBert),
		WithInfo(kitlog.With(logger, "bot", "bert")),
		WithRepoPath(filepath.Join(tRepoPath, "bert")),
		WithHMACSigning(hk),
		WithListenAddr(":0"),
	)
	r.NoError(err)
	botgroup.Go(bs.Serve(bertBot))

	// make main want it
	_, err = mainbot.PublishLog.Publish(ssb.NewContactFollow(kpBert.Id))
	r.NoError(err)

	_, err = bertBot.PublishLog.Publish(ssb.NewContactFollow(mainbot.KeyPair.Id))
	r.NoError(err)

	var msgCnt = testMessageCount
	if testing.Short() {
		msgCnt = 50
	}
	for i := msgCnt; i > 0; i-- {
		_, err = bertBot.PublishLog.Publish(i)
		r.NoError(err)
	}

	feedsMlog, ok := mainbot.GetMultiLog("userFeeds")
	r.True(ok)
	bertsFeed, err := feedsMlog.Get(bertBot.KeyPair.Id.StoredAddr())
	r.NoError(err)
	seqv, err := bertsFeed.Seq().Value()
	r.NoError(err)
	r.EqualValues(-1, seqv, "should not have berts log yet")

	// setup live listener
	seqSrc, err := mutil.Indirect(mainbot.RootLog, bertsFeed).Query(
		margaret.Gt(margaret.BaseSeq(testMessageCount-1)),
		margaret.Live(true),
	)
	r.NoError(err)

	err = mainbot.Network.Connect(ctx, bertBot.Network.GetListenAddr())
	r.NoError(err)

	ctx, cancel2 := context.WithTimeout(ctx, 15*time.Second)
	_, err = seqSrc.Next(ctx)
	r.NoError(err)

	bertBot.Shutdown()
	mainbot.Shutdown()

	cancel()
	r.NoError(bertBot.Close())
	r.NoError(mainbot.Close())
	cancel2()
	r.NoError(botgroup.Wait())
}

func XTestNullFetched(t *testing.T) {
	// defer leakcheck.Check(t)
	r := require.New(t)

	ctx, cancel := context.WithCancel(context.TODO())

	os.RemoveAll("testrun")

	appKey := make([]byte, 32)
	rand.Read(appKey)
	hmacKey := make([]byte, 32)
	rand.Read(hmacKey)

	botgroup, ctx := errgroup.WithContext(ctx)
	mainLog := testutils.NewRelativeTimeLogger(nil)
	bs := newBotServer(ctx, mainLog)

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

	botgroup.Go(bs.Serve(ali))

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

	botgroup.Go(bs.Serve(bob))

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

	var msgCnt = testMessageCount
	if testing.Short() {
		msgCnt = 50
	}
	for i := msgCnt; i > 0; i-- {
		_, err = bob.PublishLog.Publish(i)
		r.NoError(err)
	}

	aliUF, ok := ali.GetMultiLog("userFeeds")
	r.True(ok)

	alisVersionOfBobsLog, err := aliUF.Get(bob.KeyPair.Id.StoredAddr())
	r.NoError(err)

	// setup live listener
	seqSrc, err := mutil.Indirect(ali.RootLog, alisVersionOfBobsLog).Query(
		margaret.Gt(margaret.BaseSeq(msgCnt-1)),
		margaret.Live(true),
	)
	r.NoError(err)

	err = bob.Network.Connect(ctx, ali.Network.GetListenAddr())
	r.NoError(err)

	qryCtx, cancel2 := context.WithTimeout(ctx, 15*time.Second)
	_, err = seqSrc.Next(qryCtx)
	r.NoError(err)

	err = ali.NullFeed(bob.KeyPair.Id)
	r.NoError(err)

	t.Error("TODO: A has an open verifySink copy with the old feed and won't allow to refetch it. this is an regression from the live-stream refactor")

	mainLog.Log("msg", "get a fresh view (should be empty now)")
	alisVersionOfBobsLog, err = aliUF.Get(bob.KeyPair.Id.StoredAddr())
	r.NoError(err)

	bobsSeqV, err := alisVersionOfBobsLog.Seq().Value()
	r.NoError(err)
	r.EqualValues(margaret.SeqEmpty, bobsSeqV.(margaret.Seq).Seq())

	mainLog.Log("msg", "sync should give us the messages again")

	seqSrc, err = mutil.Indirect(ali.RootLog, alisVersionOfBobsLog).Query(
		margaret.Gt(margaret.BaseSeq(msgCnt-10)),
		margaret.Live(true),
	)
	r.NoError(err)

	err = bob.Network.Connect(ctx, ali.Network.GetListenAddr())
	r.NoError(err)

	qryCtx, cancel3 := context.WithTimeout(ctx, 15*time.Second)
	_, err = seqSrc.Next(qryCtx)
	r.NoError(err)

	ali.Shutdown()
	bob.Shutdown()
	cancel()
	cancel2()
	cancel3()

	r.NoError(ali.Close())
	r.NoError(bob.Close())

	r.NoError(botgroup.Wait())
}
