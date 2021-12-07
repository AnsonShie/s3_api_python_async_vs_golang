package cloudstor

import (
	"bytes"
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pkg/errors"
)

type AwsS3Config struct {
	Credential *credentials.Credentials
	Region     string
	Bucket     string
	ACL        string
}

type awsS3 struct {
	s3Svc  *s3.S3
	bucket string
}

func NewAwsS3Lister(conf *AwsS3Config) Lister {
	return newAwsS3(conf)
}

func newAwsS3(conf *AwsS3Config) *awsS3 {
	var svc *s3.S3
	if conf.Credential == nil {
		svc = s3.New(session.Must(session.NewSession(&aws.Config{
			Region: aws.String(conf.Region),
		})))
	} else {
		sess := session.Must(session.NewSession())
		svc = s3.New(sess, &aws.Config{
			Credentials: conf.Credential,
			Region:      aws.String(conf.Region),
		})
	}
	return &awsS3{
		s3Svc:  svc,
		bucket: conf.Bucket,
	}
}

func (l *awsS3) ListObjects(ctx context.Context, path string, object chan string) error {

	err := l.s3Svc.ListObjectsV2PagesWithContext(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(l.bucket),
		Prefix: aws.String(path),
	}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, content := range page.Contents {
			object <- *content.Key
		}
		return !lastPage
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && errors.Is(aerr.OrigErr(), ctx.Err()) {
			return aerr.OrigErr()
		}
		return errors.Wrap(err, "fail to list objects from aws s3")
	}
	return nil
}

func createNewSession(conf *AwsS3Config) *session.Session {
	var sess *session.Session
	if conf.Credential == nil {
		sess = session.Must(session.NewSession(&aws.Config{
			Region: aws.String(conf.Region),
		}))
	} else {
		sess = session.Must(session.NewSession(&aws.Config{Region: aws.String(conf.Region), Credentials: conf.Credential}))
	}
	return sess
}

type awsS3Downloader struct {
	downloader *s3manager.Downloader
	bucket     string
}

type awsS3ListDownloader struct {
	Downloader
	Lister
}

func NewAwsS3Downloader(conf *AwsS3Config) Downloader {
	return &awsS3Downloader{
		downloader: s3manager.NewDownloader(createNewSession(conf)),
		bucket:     conf.Bucket,
	}
}

func NewAwsS3ListDownloader(conf *AwsS3Config) ListDownloader {
	return &awsS3ListDownloader{
		Downloader: NewAwsS3Downloader(conf),
		Lister:     newAwsS3(conf),
	}
}

func (d *awsS3Downloader) Download(ctx context.Context, path string) ([]byte, error) {
	buf := &aws.WriteAtBuffer{}
	if _, err := d.downloader.DownloadWithContext(ctx, buf, &s3.GetObjectInput{
		Bucket: aws.String(d.bucket),
		Key:    aws.String(path),
	}); err != nil {
		if aerr, ok := err.(awserr.Error); ok && errors.Is(aerr.OrigErr(), ctx.Err()) {
			return nil, aerr.OrigErr()
		}
		return nil, errors.Wrap(err, "fail to get object from aws s3")
	}
	return buf.Bytes(), nil
}

type awsS3Uploader struct {
	uploader *s3manager.Uploader
	bucket   string
	acl      string
}

func NewAwsS3Uploader(conf *AwsS3Config) Uploader {
	return &awsS3Uploader{
		uploader: s3manager.NewUploader(createNewSession(conf)),
		bucket:   conf.Bucket,
		acl:      conf.ACL,
	}
}

func (u *awsS3Uploader) UploadBuffer(ctx context.Context, path string, payload []byte) error {
	_, err := u.uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Body:   bytes.NewReader(payload),
		Bucket: aws.String(u.bucket),
		Key:    aws.String(path),
		ACL:    &u.acl,
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && errors.Is(aerr.OrigErr(), ctx.Err()) {
			return aerr.OrigErr()
		}
		return errors.Wrap(err, "fail to upload buffer to aws s3")
	}
	return nil
}
