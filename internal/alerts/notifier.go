package alerts

import (
	"sync"
	"time"
)

const (
	maxRetries      = 3
	defaultMinSilence = 5 * time.Minute
	retryBaseDelay  = 2 * time.Second
)

type notificationChannel string

const (
	ChannelEmail    notificationChannel = "email"
	ChannelSlack    notificationChannel = "slack"
	ChannelTelegram notificationChannel = "telegram"
	ChannelWebhook  notificationChannel = "webhook"
)

type silenceKey struct {
	userID   string
	systemID string
	alertName string
	channel  notificationChannel
}

type Notifier struct {
	mu            sync.RWMutex
	silenceMap    map[silenceKey]time.Time
	minSilence    time.Duration
}

func NewNotifier() *Notifier {
	return &Notifier{
		silenceMap: make(map[silenceKey]time.Time),
		minSilence: defaultMinSilence,
	}
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
	key := silenceKey{
		userID:    userID,
		systemID:  systemID,
		alertName: alertName,
		channel:   channel,
	}
	n.mu.Lock()
	n.silenceMap[key] = time.Now()
	n.mu.Unlock()
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
