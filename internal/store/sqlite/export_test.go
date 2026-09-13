package sqlite

// SetJobRunningForTest transitions a job into RUNNING with attempt=1.
func (s *SQLiteStore) SetJobRunningForTest(tenantID, id string, fencingGen int64, leaseToken string) error {
	return s.SetJobRunningWithAttemptForTest(tenantID, id, fencingGen, leaseToken, 1)
}

// SetJobRunningWithAttemptForTest transitions a job into RUNNING with a specific attempt number.
func (s *SQLiteStore) SetJobRunningWithAttemptForTest(tenantID, id string, fencingGen int64, leaseToken string, attempt int) error {
	query := `
	UPDATE jobs
	SET status = 'RUNNING',
	    fencing_generation = ?,
	    lease_token = ?,
	    attempt = ?
	WHERE tenant_id = ? AND id = ?
	`
	_, err := s.db.Exec(query, fencingGen, leaseToken, attempt, tenantID, id)
	return err
}
