// Package mogdb defines and registers usql's MogDB driver.
//
// See: https://gitee.com/opengauss/openGauss-connector-go-pq
// Group: base
package mogdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/xo/usql/env"
	"io"
	"regexp"
	"strconv"
	"strings"
	
	"gitee.com/opengauss/openGauss-connector-go-pq" // DRIVER
	"github.com/xo/dburl"
	"github.com/xo/usql/drivers"
	"github.com/xo/usql/drivers/metadata"
	mgmeta "github.com/xo/usql/drivers/metadata/mogdb"
	"github.com/xo/usql/text"
)

func init() {
	openConn := func(stdout, stderr func() io.Writer, dsn string) (*sql.DB, error) {
		conn, err := pq.NewConnector(dsn)
		if err != nil {
			return nil, err
		}
		noticeConn := pq.ConnectorWithNoticeHandler(conn, func(notice *pq.Error) {
			out := stderr()
			fmt.Fprintln(out, notice.Severity+": ", notice.Message)
			if notice.Hint != "" {
				fmt.Fprintln(out, "HINT: ", notice.Hint)
			}
		})
		notificationConn := pq.ConnectorWithNotificationHandler(noticeConn, func(notification *pq.Notification) {
			var payload string
			if notification.Extra != "" {
				payload = fmt.Sprintf(text.NotificationPayload, notification.Extra)
			}
			fmt.Fprintln(stdout(), fmt.Sprintf(text.NotificationReceived, notification.Channel, payload, notification.BePid))
		})
		return sql.OpenDB(notificationConn), nil
	}
	drivers.Register("mogdb", drivers.Driver{
		Name:                   "mg",
		AllowDollar:            true,
		AllowMultilineComments: true,
		LexerName:              "mogdb",
		ForceParams: func(u *dburl.URL) {
			_, ok := u.Query()["sslmode"]
			if ok {
				return
			}
			drivers.ForceQueryParameters([]string{"sslmode", "disable"})(u)
		},
		Open: func(ctx context.Context, u *dburl.URL, stdout, stderr func() io.Writer) (func(string, string) (*sql.DB, error), error) {
			return func(_, dsn string) (*sql.DB, error) {
				conn, err := openConn(stdout, stderr, dsn)
				if err != nil {
					return nil, err
				}
				// special retry handling case, since there's no lib/pq retry mode
				if env.Get("SSLMODE") == "retry" && !u.Query().Has("sslmode") {
					switch err = conn.PingContext(ctx); {
					case errors.Is(err, pq.ErrSSLNotSupported):
						s := "sslmode=disable " + dsn
						conn, err = openConn(stdout, stderr, s)
						if err != nil {
							return nil, err
						}
						u.DSN = s
					case err != nil:
						return nil, err
					}
				}
				return conn, nil
			}, nil
		},
		Version: func(ctx context.Context, db drivers.DB) (string, error) {
			var ver string
			err := db.QueryRowContext(ctx, `select version()`).Scan(&ver)
			if err != nil {
				return "", err
			}
			dbVendor, dbVersion := parseVersion(ver)

			return dbVendor + " " + dbVersion, nil
		},
		ChangePassword: func(db drivers.DB, user, newpw, _ string) error {
			_, err := db.Exec(`ALTER USER ` + user + ` PASSWORD '` + newpw + `'`)
			return err
		},
		Err: func(err error) (string, string) {
			if e, ok := err.(*pq.Error); ok {
				return string(e.Code), e.Message
			}
			return "", err.Error()
		},
		IsPasswordErr: func(err error) bool {
			if e, ok := err.(*pq.Error); ok {
				return e.Code.Name() == "invalid_password"
			}
			return false
		},
		NewMetadataReader: mgmeta.NewReader(),
		NewMetadataWriter: func(db drivers.DB, w io.Writer, opts ...metadata.ReaderOption) metadata.Writer {
			return metadata.NewDefaultWriter(mgmeta.NewReader()(db, opts...))(db, w)
		},
		Copy: func(ctx context.Context, db *sql.DB, rows *sql.Rows, table string) (int64, error) {
			columns, err := rows.Columns()
			if err != nil {
				return 0, fmt.Errorf("failed to fetch source rows columns: %w", err)
			}
			clen := len(columns)

			query := table
			if !strings.HasPrefix(strings.ToLower(query), "insert into") {
				leftParen := strings.IndexRune(table, '(')
				colQuery := "SELECT * FROM " + table + " WHERE 1=0"
				if leftParen != -1 {
					colQuery = "SELECT " + table[leftParen+1:len(table)-1] + " FROM " + table[:leftParen] + " WHERE 1=0"
					table = table[:leftParen]
				}
				colStmt, err := db.PrepareContext(ctx, colQuery)
				if err != nil {
					return 0, fmt.Errorf("failed to prepare query to determine target table columns: %w", err)
				}
				defer colStmt.Close()
				colRows, err := colStmt.QueryContext(ctx)
				if err != nil {
					return 0, fmt.Errorf("failed to execute query to determine target table columns: %w", err)
				}
				columns, err := colRows.Columns()
				if err != nil {
					return 0, fmt.Errorf("failed to fetch target table columns: %w", err)
				}
				query = pq.CopyIn(table, columns...)
			}
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return 0, fmt.Errorf("failed to begin transaction: %w", err)
			}
			stmt, err := tx.PrepareContext(ctx, query)
			if err != nil {
				return 0, fmt.Errorf("failed to prepare insert query: %w", err)
			}
			defer stmt.Close()

			values := make([]interface{}, clen)
			for i := 0; i < clen; i++ {
				values[i] = new(interface{})
			}

			var n int64
			for rows.Next() {
				err = rows.Scan(values...)
				if err != nil {
					return n, fmt.Errorf("failed to scan row: %w", err)
				}
				_, err := stmt.ExecContext(ctx, values...)
				if err != nil {
					return n, fmt.Errorf("failed to exec copy: %w", err)
				}
			}
			res, err := stmt.ExecContext(ctx)
			if err != nil {
				return n, fmt.Errorf("failed to final exec copy: %w", err)
			}
			rn, err := res.RowsAffected()
			if err != nil {
				return n, fmt.Errorf("failed to check rows affected: %w", err)
			}
			n += rn

			err = tx.Commit()
			if err != nil {
				return n, fmt.Errorf("failed to commit transaction: %w", err)
			}

			return n, rows.Err()
		},
	})
}

var (
	gaussDBVerRep   = regexp.MustCompile(`(GaussDB|MogDB)\s+Kernel\s+V(\w+)`)
	openGaussVerRep = regexp.MustCompile(`(openGauss|MogDB)\s+([^\s]+)`)
	vastbaseVerRep  = regexp.MustCompile(`(Vastbase\s+G100)\s+V(\d+\.\d+)`)
)

func parseVersion(versionString string) (string, string) {
	versionString = strings.TrimSpace(versionString)
	if gaussDBVerRep.MatchString(versionString) {
		return parseGaussDBVersion(gaussDBVerRep.FindStringSubmatch(versionString))
	}
	if openGaussVerRep.MatchString(versionString) {
		return parseOpenGaussVersion(openGaussVerRep.FindStringSubmatch(versionString))
	}
	// if vastbaseVerRep.MatchString(versionString) {
	// 	return parseVastbaseVersion(vastbaseVerRep.FindStringSubmatch(versionString))
	// }

	return "", ""
}

func parseOpenGaussVersion(subMatches []string) (string, string) {
	if len(subMatches) < 3 || subMatches[2] == "" {
		return "", ""
	}
	return subMatches[1], subMatches[2]
}

// func parseVastbaseVersion(subMatches []string) (string, string) {
// 	if len(subMatches) < 3 || subMatches[2] == "" {
// 		return "", ""
// 	}
// 	return subMatches[1], subMatches[2]
// }

func parseGaussDBVersion(subMatches []string) (string, string) {
	if len(subMatches) < 3 || subMatches[2] == "" {
		return "", ""
	}
	r := regexp.MustCompile(`(\d+)R(\d+)C(\d+)`).FindStringSubmatch(subMatches[2])
	if len(r) < 3 {
		return "", ""
	}
	r1, _ := strconv.Atoi(r[1])
	r2, _ := strconv.Atoi(r[2])
	r3, _ := strconv.Atoi(r[3])
	return subMatches[1], fmt.Sprintf("%v.%v.%v", r1, r2, r3)
}
