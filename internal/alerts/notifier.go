package alerts

import (
	"sync"
	"time"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase/core"
)

const (
	maxRetries         = 3
	defaultMinSilence  = 5 * time.Minute
	retryBaseDelay     = 2 * time.Second
)

type notificationChannel string

const (
	ChannelEmail    notificationChannel = "email"
	ChannelSlack    notificationChannel = "slack"
	ChannelTelegram notificationChannel = "telegram"
	ChannelWebhook  notificationChannel = "webhook"
)

type silenceKey struct {
	userID    string
	systemID  string
	alertName string
	channel   notificationChannel
}

type NotificationFailure struct {
	Channel notificationChannel
	Reason  string
	Time    time.Time
}

type Notifier struct {
	mu            sync.RWMutex
	silenceMap    map[silenceKey]time.Time
	minSilence    time.Duration
	app           core.App
}

func NewNotifier(app core.App) *Notifier {
	n := &Notifier{
		silenceMap: make(map[silenceKey]time.Time),
		minSilence: defaultMinSilence,
		app:        app,
	}
	_ = n.loadSilencesFromDB()
	return n
}

func (n *Notifier) SetMinSilence(d time.Duration) {
	if d < defaultMinSilence {
		d = defaultMinSilence
	}
	n.mu.Lock()
	n.minSilence = d
	n.mu.Unlock()
}

func (n *Notifier) IsSilenced(userID, systemID, alertName string, channel notificationChannel) bool {
	key := silenceKey{
		userID:    userID,
		systemID:  systemID,
		alertName: alertName,
		channel:   channel,
	}
	n.mu.RLock()
	lastSent, ok := n.silenceMap[key]
	n.mu.RUnlock()
	if !ok {
		return false
	}
	return time.Since(lastSent) < n.minSilence
}

func (n *Notifier) MarkSent(userID, systemID, alertName string, channel notificationChannel) {
	now := time.Now()
	key := silenceKey{
		userID:    userID,
		systemID:  systemID,
		alertName: alertName,
		channel:   channel,
	}
	n.mu.Lock()
	n.silenceMap[key] = now
	n.mu.Unlock()
	_ = n.persistSilenceToDB(userID, systemID, alertName, string(channel), now)
}

func (n *Notifier) RecordFailure(userID, systemID, alertName string, channel notificationChannel, reason string) {
	type failureRecord struct {
		User      string    `db:"user"`
		System    string    `db:"system"`
		AlertName string    `db:"alert_name"`
		Channel   string    `db:"channel"`
		Reason    string    `db:"reason"`
		Created   time.Time `db:"created"`
	}
	if n.app == nil {
		return
	}
	_, _ = n.app.DB().
		Insert("notification_failures", dbx.Params{
			"user":       userID,
			"system":     systemID,
			"alert_name": alertName,
			"channel":    string(channel),
			"reason":     reason,
			"created":    time.Now().UTC(),
		}).
		Execute()
}

func (n *Notifier) GetFailures(userID string, since time.Duration) []NotificationFailure {
	if n.app == nil {
		return nil
	}
	type dbRecord struct {
		Channel   string    `db:"channel"`
		Reason    string    `db:"reason"`
		Created   time.Time `db:"created"`
	}
	var records []dbRecord
	_ = n.app.DB().
		Select("channel", "reason", "created").
		From("notification_failures").
		Where(dbx.NewExp("user = {:user} AND created > {:since}", dbx.Params{
			"user":  userID,
			"since": time.Now().UTC().Add(-since),
		})).
		OrderBy("created DESC").
		Limit(100).
		All(&records)

	failures := make([]NotificationFailure, 0, len(records))
	for _, r := range records {
		failures = append(failures, NotificationFailure{
			Channel: notificationChannel(r.Channel),
			Reason:  r.Reason,
			Time:    r.Created,
		})
	}
	return failures
}

func (n *Notifier) CleanupOldSilences() {
	n.mu.Lock()
	defer n.mu.Unlock()
	cutoff := time.Now().Add(-n.minSilence * 2)
	for k, t := range n.silenceMap {
		if t.Before(cutoff) {
			delete(n.silenceMap, k)
		}
	}
	if n.app != nil {
		_, _ = n.app.DB().
			Delete("notification_silences", dbx.NewExp("last_sent < {:cutoff}", dbx.Params{
				"cutoff": cutoff.UTC(),
			})).
			Execute()
	}
}

func (n *Notifier) loadSilencesFromDB() error {
	if n.app == nil {
		return nil
	}
	type dbSilence struct {
		UserID    string    `db:"user"`
		SystemID  string    `db:"system"`
		AlertName string    `db:"alert_name"`
		Channel   string    `db:"channel"`
		LastSent  time.Time `db:"last_sent"`
	}
	var records []dbSilence
	cutoff := time.Now().UTC().Add(-defaultMinSilence)
	err := n.app.DB().
		Select("user", "system", "alert_name", "channel", "last_sent").
		From("notification_silences").
		Where(dbx.NewExp("last_sent >= {:cutoff}", dbx.Params{"cutoff": cutoff})).
		All(&records)
	if err != nil {
		return err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, r := range records {
		key := silenceKey{
			userID:    r.UserID,
			systemID:  r.SystemID,
			alertName: r.AlertName,
			channel:   notificationChannel(r.Channel),
		}
		n.silenceMap[key] = r.LastSent
	}
	return nil
}

func (n *Notifier) persistSilenceToDB(userID, systemID, alertName, channel string, t time.Time) error {
	if n.app == nil {
		return nil
	}
	existing := struct {
		ID string `db:"id"`
	}{}
	err := n.app.DB().
		Select("id").
		From("notification_silences").
		Where(dbx.NewExp(
			"user = {:user} AND system = {:system} AND alert_name = {:alert} AND channel = {:channel}",
			dbx.Params{
				"user":    userID,
				"system":  systemID,
				"alert":   alertName,
				"channel": channel,
			})).
		One(&existing)

	if err == nil && existing.ID != "" {
		_, err = n.app.DB().
			Update("notification_silences", dbx.Params{"last_sent": t.UTC()}, dbx.NewExp("id = {:id}", dbx.Params{"id": existing.ID})).
			Execute()
		return err
	}

	_, err = n.app.DB().
		Insert("notification_silences", dbx.Params{
			"user":       userID,
			"system":     systemID,
			"alert_name": alertName,
			"channel":    channel,
			"last_sent":  t.UTC(),
		}).
		Execute()
	return err
}

func (am *AlertManager) sendWithRetry(channel notificationChannel, sendFn func() error) error {
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		err := sendFn()
		if err == nil {
			return nil
		}
		lastErr = err
		if i < maxRetries-1 {
			delay := retryBaseDelay * time.Duration(1<<uint(i))
			time.Sleep(delay)
		}
	}
	return lastErr
}

func detectChannelType(url string) notificationChannel {
	if len(url) >= 8 && url[:8] == "slack://" {
		return ChannelSlack
	}
	if len(url) >= 11 && url[:11] == "telegram://" {
		return ChannelTelegram
	}
	return ChannelWebhook
}
