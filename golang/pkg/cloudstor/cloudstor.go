package cloudstor

import (
	"context"
)

type ListDownloader interface {
	Downloader
	Lister
}

type Lister interface {
	ListObjects(ctx context.Context, path string, object chan string) error
}

type Downloader interface {
	Download(ctx context.Context, path string) ([]byte, error)
}

type Uploader interface {
	UploadBuffer(ctx context.Context, path string, payload []byte) error
}
