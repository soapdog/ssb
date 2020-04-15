package sbot

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"go.cryptoscope.co/margaret"
	"go.cryptoscope.co/ssb"
	"go.cryptoscope.co/ssb/internal/mutil"
	"go.cryptoscope.co/ssb/internal/testutils"
)

func TestFeedsLiveReconnect(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)
	os.RemoveAll(filepath.Join("testrun", t.Name()))

	ctx, cancel := ShutdownContext(context.Background())
	botgroup, ctx := errgroup.WithContext(ctx)

	info := testutils.NewRelativeTimeLogger(nil)
	bs := newBotServer(ctx, info)

	appKey := make([]byte, 32)
	rand.Read(appKey)
	hmacKey := make([]byte, 32)
	rand.Read(hmacKey)

	netOpts := []Option{
		WithAppKey(appKey),
		WithHMACSigning(hmacKey),
	}

	botA := makeNamedTestBot(t, "A", netOpts)
	botgroup.Go(bs.Serve(botA))

	botI := makeNamedTestBot(t, "I", netOpts)
	botgroup.Go(bs.Serve(botI))

	var bLeafs []*Sbot
	botCnt := 8
	for i := 0; i < botCnt; i++ {
		botBi := makeNamedTestBot(t, fmt.Sprintf("B%0d", i), netOpts)
		botgroup.Go(bs.Serve(botBi))
		bLeafs = append(bLeafs, botBi)
	}

	theBots := []*Sbot{botA, botI} // all the bots
	theBots = append(theBots, bLeafs...)

	// be-friend the network
	_, err := botA.PublishLog.Append(ssb.NewContactFollow(botI.KeyPair.Id))
	r.NoError(err)
	_, err = botI.PublishLog.Append(ssb.NewContactFollow(botA.KeyPair.Id))
	r.NoError(err)
	var msgCnt = 2

	for i, bot := range bLeafs {
		_, err := bot.PublishLog.Append(ssb.NewContactFollow(botI.KeyPair.Id))
		r.NoError(err, "follow b%d>I failed", i)
		_, err = botI.PublishLog.Append(ssb.NewContactFollow(bot.KeyPair.Id))
		r.NoError(err, "follow I>b%d failed", i)
		msgCnt += 2
	}

	var extraTestMessages = 256
	if testing.Short() {
		extraTestMessages = 25
	}
	msgCnt += extraTestMessages
	for n := extraTestMessages; n > 0; n-- {
		tMsg := fmt.Sprintf("some pre-setup msg %d", n)
		_, err := botA.PublishLog.Append(tMsg)
		r.NoError(err)
	}

	initialSync(t, theBots, msgCnt)

	seqOfFeedA := margaret.BaseSeq(extraTestMessages) // N pre messages +1 contact (0 indexed)

	// did Bi get feed A?
	botB0 := bLeafs[0]
	feedIdxOfB0, ok := botB0.GetMultiLog("userFeeds")
	r.True(ok)
	feedAonBotB, err := feedIdxOfB0.Get(botA.KeyPair.Id.StoredAddr())
	r.NoError(err)
	seqv, err := feedAonBotB.Seq().Value()
	r.NoError(err)
	a.EqualValues(seqOfFeedA, seqv, "botB0 should have all of A's messages")

	// setup live listener
	liveQry, err := mutil.Indirect(botB0.RootLog, feedAonBotB).Query(
		margaret.Gt(seqOfFeedA),
		margaret.Live(true),
	)
	r.NoError(err)

	t.Log("starting live test")
	// connect all bots to I
	for i, botX := range append(bLeafs, botA) {
		err := botX.Network.Connect(ctx, botI.Network.GetListenAddr())
		r.NoError(err, "connect bot%d>I failed", i)
	}
	for i := 0; i < extraTestMessages; i++ {
		tMsg := fmt.Sprintf("some fresh msg %d", i)
		seq, err := botA.PublishLog.Append(tMsg)
		r.NoError(err)
		r.EqualValues(msgCnt+i, seq, "new msg %d", i)

		if i%9 == 0 { // simulate faulty network
			botX := i%(botCnt-1) + 1 // some of the other (b[0] keeps receiveing)
			dcBot := bLeafs[botX]
			ct := dcBot.Network.GetConnTracker()
			ct.CloseAll()
			t.Log("disconnecting", botX)
			go func(b *Sbot) {
				time.Sleep(time.Second / 4)
				timeoutCtx, _ := context.WithTimeout(ctx, 1*time.Minute)
				err := b.Network.Connect(timeoutCtx, botI.Network.GetListenAddr())
				r.NoError(err)
				// cancel()
			}(dcBot)
		}

		// received new message?
		timeoutCtx, cancel := context.WithTimeout(ctx, time.Second)
		v, err := liveQry.Next(timeoutCtx)
		cancel()
		if err != nil {
			t.Error("liveQry err", err)
			continue
		}

		msg, ok := v.(ssb.Message)
		r.True(ok, "got %T", v)

		a.EqualValues(int(seqOfFeedA+2)+i, msg.Seq(), "botB0: wrong seq")
	}

	time.Sleep(time.Second * 3)
	cancel()

	finalWantSeq := seqOfFeedA + margaret.BaseSeq(extraTestMessages)
	for i, bot := range bLeafs {

		// did Bi get feed A?
		ufOfBotB, ok := bot.GetMultiLog("userFeeds")
		r.True(ok)

		feedAonBotB, err := ufOfBotB.Get(botA.KeyPair.Id.StoredAddr())
		r.NoError(err)

		seqv, err := feedAonBotB.Seq().Value()
		r.NoError(err)
		a.EqualValues(finalWantSeq, seqv, "botB%02d should have all of A's messages", i)
	}

	// cleanup
	time.Sleep(1 * time.Second)
	for bI, bot := range append(bLeafs, botA, botI) {
		err = bot.FSCK(FSCKWithMode(FSCKModeSequences))
		a.NoError(err, "botB%02d fsck", bI)
		bot.Shutdown()
		r.NoError(bot.Close(), "closed botB%02d failed", bI)
	}
	r.NoError(botgroup.Wait())
}
