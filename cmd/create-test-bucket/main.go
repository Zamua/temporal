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
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("admin", "driftwood-dev", "")),
	)
	if err != nil {
		fmt.Println("load config:", err)
		os.Exit(1)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String("http://127.0.0.1:9000")
		o.UsePathStyle = true
	})
	bucket := "temporal-conformance"
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	if err != nil {
		var alreadyExists *s3types.BucketAlreadyOwnedByYou
		if !errAs(err, &alreadyExists) {
			fmt.Printf("create bucket: %v\n", err)
			// Continue if it already exists.
		}
	}
	// List bucket to confirm.
	out, err := client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		fmt.Println("list:", err)
		os.Exit(1)
	}
	fmt.Println("Buckets:")
	for _, b := range out.Buckets {
		fmt.Println(" -", aws.ToString(b.Name))
	}
}

func errAs(err error, target interface{}) bool {
	if err == nil {
		return false
	}
	type asInterface interface {
		As(target interface{}) bool
	}
	if ai, ok := err.(asInterface); ok {
		return ai.As(target)
	}
	return false
}
