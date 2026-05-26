package main

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func main() {
	ctx := context.Background()
	cfg, _ := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("admin", "driftwood-dev", "")),
	)
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("http://127.0.0.1:9000")
		o.UsePathStyle = true
	})
	bucket := "temporal-conformance"
	var token *string
	deleted := 0
	for {
		page, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			ContinuationToken: token,
		})
		if err != nil { fmt.Println(err); os.Exit(1) }
		var objs []s3types.ObjectIdentifier
		for _, o := range page.Contents {
			objs = append(objs, s3types.ObjectIdentifier{Key: o.Key})
		}
		if len(objs) > 0 {
			_, _ = client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
				Bucket: aws.String(bucket),
				Delete: &s3types.Delete{Objects: objs},
			})
			deleted += len(objs)
		}
		if !aws.ToBool(page.IsTruncated) { break }
		token = page.NextContinuationToken
	}
	fmt.Println("Deleted", deleted, "objects")
}
