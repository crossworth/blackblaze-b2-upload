package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"

	"github.com/kurin/blazer/b2"
)

func main() {
	log.SetOutput(os.Stdout)

	var (
		keyID      string
		appID      string
		bucketName string
		writers    int
		src        string
		dst        string
	)

	flag.StringVar(&keyID, "keyID", "", "BlackBlaze KeyID")
	flag.StringVar(&appID, "appID", "", "BlackBlaze AppID")
	flag.StringVar(&bucketName, "bucketName", "", "BlackBlaze Bucket name")
	flag.StringVar(&src, "src", "", "Source (file or folder)")
	flag.StringVar(&dst, "dst", "", "Destination (file or folder)")
	flag.IntVar(&writers, "writers", 10, "Number of writes")

	flag.Parse()

	if keyID == "" {
		log.Fatalf("you must provide the keyID")
	}

	if appID == "" {
		log.Fatalf("you must provide the appID")
	}

	if bucketName == "" {
		log.Fatalf("you must provide the bucketName")
	}

	if src == "" {
		log.Fatalf("you must provide the src")
	}

	if dst == "" {
		log.Fatalf("you must provide the dst")
	}

	isDir := checkFileIsDir(src)

	ctx := context.Background()
	b2Client, err := b2.NewClient(ctx, appID, keyID)
	if err != nil {
		log.Fatalf("could not create BlackBlaze client %v", err)
	}

	bucket, err := b2Client.Bucket(ctx, bucketName)
	if err != nil {
		log.Fatalf("could not get the bucket %q: %v", bucketName, err)
	}

	var files []string

	if isDir {
		dirFiles, err := os.ReadDir(src)
		if err != nil {
			log.Fatalf("could not read files inside directory: %v", err)
		}
		for _, f := range dirFiles {
			files = append(files, path.Join(src, f.Name()))
		}
	} else {
		files = append(files, src)
	}

	total := len(files)
	for i, f := range files {
		baseName := filepath.Base(f)
		dst := fmt.Sprintf("%s/%s", dst, baseName)
		log.Printf("[%d/%d] - uploading: %s\n", i+1, total, f)
		err := uploadFile(ctx, bucket, f, dst, writers)
		if err != nil {
			log.Fatalf("could not upload file %q to bucket: %v", f, err)
		}
	}

	log.Printf("done\n")
}

func checkFileIsDir(src string) bool {
	fileInfo, err := os.Stat(src)
	if err != nil {
		log.Fatalf("could not read source,: %v", err)
	}

	return fileInfo.IsDir()
}

func uploadFile(ctx context.Context, bucket *b2.Bucket, src, dst string, writers int) error {
	f, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("could not open the file %q: %w", src, err)
	}
	defer f.Close()

	w := bucket.Object(dst).NewWriter(ctx)
	w.ConcurrentUploads = writers
	if _, err := io.Copy(w, f); err != nil {
		_ = w.Close()
		return err
	}
	return w.Close()
}
