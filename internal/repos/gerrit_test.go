package repos

import (
	"context"
	"testing"

	"github.com/sourcegraph/sourcegraph/internal/extsvc"
	"github.com/sourcegraph/sourcegraph/internal/httpcli"
	"github.com/sourcegraph/sourcegraph/internal/testutil"
	"github.com/sourcegraph/sourcegraph/internal/types"
	"github.com/sourcegraph/sourcegraph/schema"
)

func TestGerritSource_ListRepos(t *testing.T) {
	conf := &schema.GerritConnection{
		Url: "https://gerrit-review.googlesource.com",
	}
	cf, save := newClientFactory(t, t.Name(), httpcli.GerritUnauthenticateMiddleware)
	defer save(t)

	svc := &types.ExternalService{
		Kind:   extsvc.KindGerrit,
		Config: extsvc.NewUnencryptedConfig(marshalJSON(t, conf)),
	}

	ctx := context.Background()
	src, err := NewGerritSource(ctx, svc, cf)
	if err != nil {
		t.Fatal(err)
	}

	src.perPage = 25

	repos, err := listAll(context.Background(), src)
	if err != nil {
		t.Fatal(err)
	}

	testutil.AssertGolden(t, "testdata/sources/GERRIT/"+t.Name(), update(t.Name()), repos)
}
