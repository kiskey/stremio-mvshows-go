package database

import (
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func getDB(tx *gorm.DB) *gorm.DB {
	if tx != nil {
		return tx
	}
	return DB
}

// FindThreadByHash retrieves a Thread by its unique thread_hash.
func FindThreadByHash(hash string) (*Thread, error) {
	var t Thread
	err := DB.Where("thread_hash = ?", hash).First(&t).Error
	if err == gorm.ErrRecordNotFound {
		return nil, nil
	}
	return &t, err
}

// FindThreadByRawTitle retrieves a Thread by its exact raw_title.
func FindThreadByRawTitle(rawTitle string) (*Thread, error) {
	var t Thread
	err := DB.Where("raw_title = ?", rawTitle).First(&t).Error
	if err == gorm.ErrRecordNotFound {
		return nil, nil
	}
	return &t, err
}

// CreateOrUpdateThread upserts a Thread using ON CONFLICT logic.
func CreateOrUpdateThread(data *Thread, tx *gorm.DB) error {
	db := getDB(tx)
	return db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "thread_hash"}},
		UpdateAll: true,
	}).Create(data).Error
}

// LogFailedThread logs or updates a failed thread entry.
func LogFailedThread(hash, rawTitle, reason string, tx *gorm.DB) error {
	db := getDB(tx)
	return db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "thread_hash"}},
		DoUpdates: clause.AssignmentColumns([]string{"raw_title", "reason", "last_attempt"}),
	}).Create(&FailedThread{
		ThreadHash:  hash,
		RawTitle:    rawTitle,
		Reason:      reason,
		LastAttempt: time.Now(),
	}).Error
}

// FindSeriesStreams finds quality-ordered streams matching the tmdb_id, season, and exact episode within range boundaries.
func FindSeriesStreams(tmdbID string, season, episode int) ([]Stream, error) {
	var streams []Stream
	err := DB.Where("tmdb_id = ? AND season = ? AND episode <= ? AND episode_end >= ?",
		tmdbID, season, episode, episode).
		Order("quality DESC").
		Find(&streams).Error
	return streams, err
}

// FindMovieStreams retrieves all streams for a movie tmdb_id.
func FindMovieStreams(tmdbID string) ([]Stream, error) {
	var streams []Stream
	err := DB.Where("tmdb_id = ?", tmdbID).
		Order("quality DESC").
		Find(&streams).Error
	return streams, err
}

// CreateStreams batch creates multiple streams, ignoring duplicates on duplicate unique index conflict.
func CreateStreams(streams []Stream, tx *gorm.DB) error {
	if len(streams) == 0 {
		return nil
	}
	db := getDB(tx)
	return db.Clauses(clause.OnConflict{
		DoNothing: true,
	}).Create(&streams).Error
}

// DeleteThread deletes a Thread and any associated Stream records in a cascade.
func DeleteThread(t *Thread, tx *gorm.DB) error {
	db := getDB(tx)
	return db.Delete(t).Error
}

// GetPendingThreads gets all threads marked with "pending_tmdb".
func GetPendingThreads() ([]Thread, error) {
	var threads []Thread
	err := DB.Where("status = ?", "pending_tmdb").Order("posted_at DESC").Find(&threads).Error
	return threads, err
}

// GetFailedThreads retrieves all recorded parsing/workflow failures.
func GetFailedThreads() ([]FailedThread, error) {
	var failures []FailedThread
	err := DB.Order("last_attempt DESC").Find(&failures).Error
	return failures, err
}

// DeleteFailedThread deletes an entry from the failed_threads table by its hash.
func DeleteFailedThread(hash string, tx *gorm.DB) error {
	db := getDB(tx)
	return db.Where("thread_hash = ?", hash).Delete(&FailedThread{}).Error
}
