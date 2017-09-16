package localstore

import (
	"context"
	"errors"

	sourcegraph "sourcegraph.com/sourcegraph/sourcegraph/pkg/api"
)

type orgMembers struct{}

func (*orgMembers) Create(ctx context.Context, orgID int32, userID, username, email, displayName, avatarURL string) (*sourcegraph.OrgMember, error) {
	m := sourcegraph.OrgMember{
		OrgID:       orgID,
		UserID:      userID,
		Username:    username,
		Email:       email,
		DisplayName: displayName,
		AvatarURL:   avatarURL,
	}
	err := globalDB.QueryRow(
		"INSERT INTO org_members(org_id, user_id, username, email, display_name, avatar_url) VALUES($1, $2, $3, $4, $5, $6) RETURNING id, created_at, updated_at",
		m.OrgID, m.UserID, m.Username, m.Email, m.DisplayName, m.AvatarURL).Scan(&m.ID, &m.CreatedAt, &m.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

func (om *orgMembers) GetByUserID(ctx context.Context, orgID int32, userID string) (*sourcegraph.OrgMember, error) {
	return om.getOneBySQL(ctx, "WHERE org_id=$1 user_id=$2 LIMIT 1", orgID, userID)
}

func (om *orgMembers) GetByEmail(ctx context.Context, orgID int32, email string) (*sourcegraph.OrgMember, error) {
	return om.getOneBySQL(ctx, "WHERE org_id=$1 email=$2 LIMIT 1", orgID, email)
}

func (*orgMembers) Remove(ctx context.Context, orgID int32, userID string) error {
	_, err := globalDB.Exec("DELETE FROM org_members WHERE (org_id=$1 AND user_id=$2)", orgID, userID)
	return err
}

// GetByOrg returns a list of all members of a given organization.
func (*orgMembers) GetByOrgID(ctx context.Context, orgID int32) ([]*sourcegraph.OrgMember, error) {
	org, err := Orgs.GetByID(ctx, orgID)
	if err != nil {
		return nil, err
	}
	return OrgMembers.getBySQL(ctx, "WHERE org_id=$1", org.ID)
}

// ErrOrgMemberNotFound is the error that is returned when
// a user is not in an org.
var ErrOrgMemberNotFound = errors.New("org member not found")

func (om *orgMembers) getOneBySQL(ctx context.Context, query string, args ...interface{}) (*sourcegraph.OrgMember, error) {
	members, err := om.getBySQL(ctx, query, args)
	if err != nil {
		return nil, err
	}
	if len(members) != 1 {
		return nil, ErrOrgMemberNotFound
	}
	return members[0], nil
}

func (*orgMembers) getBySQL(ctx context.Context, query string, args ...interface{}) ([]*sourcegraph.OrgMember, error) {
	rows, err := globalDB.Query("SELECT id, org_id, user_id, username, email, display_name, avatar_url, created_at, updated_at FROM org_members "+query, args...)
	if err != nil {
		return nil, err
	}

	members := []*sourcegraph.OrgMember{}
	defer rows.Close()
	for rows.Next() {
		m := sourcegraph.OrgMember{}
		err := rows.Scan(&m.ID, &m.OrgID, &m.UserID, &m.Username, &m.Email, &m.DisplayName, &m.AvatarURL, &m.CreatedAt, &m.UpdatedAt)
		if err != nil {
			return nil, err
		}
		members = append(members, &m)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return members, nil
}
