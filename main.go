package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	memfs "github.com/go-git/go-billy/v5/memfs"
	git "github.com/go-git/go-git/v5"
	diff "github.com/go-git/go-git/v5/plumbing/format/diff"
	plumbingObject "github.com/go-git/go-git/v5/plumbing/object"
	memory "github.com/go-git/go-git/v5/storage/memory"

	elasticsearch "github.com/elastic/go-elasticsearch/v8"
	esapi "github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/esutil"
)

func CheckIfError(err error) {
	if err == nil {
		return
	}

	fmt.Printf("\x1b[31;1m%s\x1b[0m\n", fmt.Sprintf("error: %s", err))
	debug.PrintStack()

	os.Exit(1)
}

type DateRange struct {
	Gte string `json:"gte,omitempty"`
	Lte string `json:"lte,omitempty"`
}

type FileRevisionDescriptor struct {
	ValidWithin  DateRange `json:"validWithin,omitempty"`
	Hash         string    `json:"hash,omitempty"`
	AuthorName   string    `json:"authorName,omitempty"`
	AuthorEmail  string    `json:"authorEmail,omitempty"`
	Date         string    `json:"date,omitempty"`
	Message      string    `json:"message,omitempty"`
	Branch       string    `json:"branch,omitempty"`
	Repository   string    `json:"repository,omitempty"`
	Size         int       `json:"size,omitempty"`
	Loc          int       `json:"loc,omitempty"`
	Extension    string    `json:"extension,omitempty"`
	Dir0         string    `json:"dir0,omitempty"`
	Dir1         string    `json:"dir1,omitempty"`
	Dir2         string    `json:"dir2,omitempty"`
	Dir3         string    `json:"dir3,omitempty"`
	Dir4         string    `json:"dir4,omitempty"`
	Dir5         string    `json:"dir5,omitempty"`
	Dir6         string    `json:"dir6,omitempty"`
	Dir7         string    `json:"dir7,omitempty"`
	Dir8         string    `json:"dir8,omitempty"`
	Dir9         string    `json:"dir9,omitempty"`
	FileName     string    `json:"fileName,omitempty"`
	Operation    string    `json:"operation,omitempty"`
	NewContent   string    `json:"newContent,omitempty"`
	OldContent   string    `json:"oldContent,omitempty"`
	OldSize      int       `json:"oldSize,omitempty"`
	OldLoc       int       `json:"oldLoc,omitempty"`
	OldPath      string    `json:"oldPath,omitempty"`
	LinesAdded   []string  `json:"linesAdded,omitempty"`
	LinesRemoved []string  `json:"linesRemoved,omitempty"`
}

const MAX_FILE_SIZE = 100000

func newTrue() *bool {
	b := true
	return &b
}

func safeGet(elements []string, index int) string {
	if len(elements) > index {
		return elements[index]
	}
	return ""
}

func getAddedLines(patch diff.FilePatch) []string {
	var added []string
	for _, element := range patch.Chunks() {
		if element.Type() == diff.Add {
			added = append(added, element.Content())
		}
	}
	return added
}

func getRemovedLines(patch diff.FilePatch) []string {
	var removed []string
	for _, element := range patch.Chunks() {
		if element.Type() == diff.Delete {
			removed = append(removed, element.Content())
		}
	}
	return removed
}

func getExtension(file string) string {
	var nameElements = strings.Split(file, ".")
	return nameElements[len(nameElements)-1]
}

func main() {
	var reponame = os.Getenv("REPO")
	var branchname = os.Getenv("BRANCH")
	years, err := strconv.Atoi(os.Getenv("MAXAGE"))

	var maxAge = time.Now().AddDate(-1*years, 0, 0)
	fmt.Println(reponame + ", " + branchname)
	fs := memfs.New()

	repo, err := git.Clone(memory.NewStorage(), fs, &git.CloneOptions{
		URL: "file:///Users/joereuter/Clones/" + reponame,
	})
	CheckIfError(err)

	branch, err := repo.Branch(branchname)
	CheckIfError(err)

	ref, err := repo.Reference(branch.Merge, true)
	CheckIfError(err)

	commit, err := repo.CommitObject(ref.Hash())

	var commits []*plumbingObject.Commit
	for commit != nil {
		commits = append(commits, commit)
		commit, err = commit.Parent(0)
		if commit.Author.When.Before(maxAge) {
			break
		}
	}
	fmt.Printf("%v commits\n", len(commits))

	writeQueue := make(chan FileRevisionDescriptor, 256)
	done := make(chan bool)
	currentFileDescriptors := make(map[string]FileRevisionDescriptor)
	go func() {
		cfg := elasticsearch.Config{
			Addresses: []string{
				os.Getenv("ES"),
			},
		}

		fmt.Println("Connect")
		es, err := elasticsearch.NewClient(cfg)
		CheckIfError(err)
		req := esapi.IndicesDeleteRequest{
			Index:             []string{"file_version_" + reponame},
			AllowNoIndices:    newTrue(),
			IgnoreUnavailable: newTrue(),
		}
		fmt.Println("Delete old index")
		res1, err := req.Do(context.Background(), es)
		CheckIfError(err)
		res1.Body.Close()
		req2 := esapi.IndicesCreateRequest{
			Index: "file_version_" + reponame,
			Body: bytes.NewReader([]byte(`{
			"mappings": {
			  "properties": {
				"hash": {
				  "type": "keyword"
				},
				"authorName": {
				  "type": "keyword"
				},
				"authorEmail": {
				  "type": "keyword"
				},
				"date": {
				  "type": "date"
				},
				"validWithin": {
				  "type": "date_range"
				},
				"message": {
				  "type": "text"
				},
				"branch": {
				  "type": "keyword"
				},
				"repository": {
				  "type": "keyword"
				},
				"fileName": {
				  "type": "keyword"
				},
				"oldFileName": {
				  "type": "keyword"
				},
				"operation": {
				  "type": "keyword"
				},
				"newContent": {
				  "type": "text"
				},
				"oldContent": {
				  "type": "text"
				},
				"linesAdded": {
				  "type": "text"
				},
				"linesRemoved": {
				  "type": "text"
				},
				"size": {
				  "type": "long"
				},
				"oldSize": {
				  "type": "long"
				},
				"oldLoc": {
				  "type": "long"
				},
				"loc": {
				  "type": "long"
				},
				"dir0": {
				  "type": "keyword"
				},
				"dir1": {
				  "type": "keyword"
				},
				"dir2": {
				  "type": "keyword"
				},
				"dir3": {
				  "type": "keyword"
				},
				"dir4": {
				  "type": "keyword"
				},
				"dir5": {
				  "type": "keyword"
				},
				"dir6": {
				  "type": "keyword"
				},
				"dir7": {
				  "type": "keyword"
				},
				"dir8": {
				  "type": "keyword"
				},
				"dir10": {
				  "type": "keyword"
				},
				"dir11": {
				  "type": "keyword"
				},
				"dir12": {
				  "type": "keyword"
				},
				"dir13": {
				  "type": "keyword"
				},
				"dir14": {
				  "type": "keyword"
				},
				"dir15": {
				  "type": "keyword"
				},
				"dir16": {
				  "type": "keyword"
				},
				"extension": {
				  "type": "keyword"
				}
			  }
			}
		  }`)),
		}
		fmt.Println("Create index " + "file_version_" + reponame)
		res2, err := req2.Do(context.Background(), es)
		CheckIfError(err)
		res2.Body.Close()
		fmt.Println("Starting bulk indexing")
		var countSuccessful uint64 = 0
		bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
			Index:         "file_version_" + reponame, // The default index name
			Client:        es,                         // The Elasticsearch client
			NumWorkers:    4,                          // The number of worker goroutines
			FlushBytes:    int(5e+5),                  // The flush threshold in bytes
			FlushInterval: 10 * time.Second,           // The periodic flush interval
		})
		CheckIfError(err)
		for {
			j, more := <-writeQueue
			data, err := json.Marshal(j)
			CheckIfError(err)
			err = bi.Add(
				context.Background(),
				esutil.BulkIndexerItem{
					// Action field configures the operation to perform (index, create, delete, update)
					Action: "index",

					// Body is an `io.Reader` with the payload
					Body: bytes.NewReader(data),

					OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
						atomic.AddUint64(&countSuccessful, 1)
						if countSuccessful%1000 == 0 {
							fmt.Printf("Uploaded %v revisions\n", countSuccessful)
						}
					},

					// OnFailure is called for each failed operation
					OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
						if err != nil {
							fmt.Printf("ERROR: %s", err)
						} else {
							fmt.Printf("ERROR: %s: %s", res.Error.Type, res.Error.Reason)
						}
					},
				},
			)
			CheckIfError(err)
			if !more {
				err = bi.Close(context.Background())
				CheckIfError(err)
				done <- true
				return
			}
		}
	}()

	var index = len(commits) - 1
	var commitDate time.Time = time.Unix(0, 0)
	for index >= 0 {
		var commit = commits[index]
		if commit.Author.When.After(commitDate) {
			commitDate = commit.Author.When
		}
		var formattedCommitDate = commitDate.Format(time.RFC3339)
		var iteration = len(commits) - index + 1
		if iteration%50 == 0 {
			fmt.Printf("Commit %v\n", iteration)
		}
		if index == (len(commits) - 1) {
			files, err := commit.Files()
			CheckIfError(err)
			files.ForEach(func(file *plumbingObject.File) error {
				var nameElements = strings.Split(file.Name, "/")
				var extension = getExtension(file.Name)
				var dir0 = safeGet(nameElements, 0)
				var dir1 = safeGet(nameElements, 1)
				var dir2 = safeGet(nameElements, 2)
				var dir3 = safeGet(nameElements, 3)
				var dir4 = safeGet(nameElements, 4)
				var dir5 = safeGet(nameElements, 5)
				var dir6 = safeGet(nameElements, 6)
				var dir7 = safeGet(nameElements, 7)
				var dir8 = safeGet(nameElements, 8)
				var dir9 = safeGet(nameElements, 9)
				var newContent = ""

				blob, err := repo.BlobObject(file.Hash)
				var newSize = int(blob.Size)
				CheckIfError(err)
				isBinary, err := file.IsBinary()
				CheckIfError(err)
				if blob.Size != 0 && blob.Size < MAX_FILE_SIZE && !isBinary {
					reader, err := blob.Reader()
					CheckIfError(err)
					var data = make([]byte, blob.Size)
					_, err = reader.Read(data)
					CheckIfError(err)
					newContent = string(data)
				}

				var fileDescriptor = FileRevisionDescriptor{
					Hash:        commit.Hash.String(),
					AuthorName:  commit.Author.Name,
					AuthorEmail: commit.Author.Email,
					Date:        formattedCommitDate,
					Message:     commit.Message,
					Branch:      branchname,
					Repository:  reponame,
					Size:        newSize,
					Loc:         len(strings.Split(newContent, "\n")),
					Extension:   extension,
					Dir0:        dir0,
					Dir1:        dir1,
					Dir2:        dir2,
					Dir3:        dir3,
					Dir4:        dir4,
					Dir5:        dir5,
					Dir6:        dir6,
					Dir7:        dir7,
					Dir8:        dir8,
					Dir9:        dir9,
					FileName:    file.Name,
					Operation:   "add",
					NewContent:  newContent,
					OldContent:  "",
					ValidWithin: DateRange{
						Gte: formattedCommitDate,
						Lte: "",
					},
					OldSize:      0,
					OldLoc:       0,
					LinesAdded:   []string{},
					LinesRemoved: []string{},
				}
				currentFileDescriptors[file.Name] = fileDescriptor
				return nil
			})
		} else {
			var prevCommit = commits[index+1]
			prevTree, err := prevCommit.Tree()
			tree, err := commit.Tree()
			CheckIfError(err)

			patch, err := prevTree.Patch(tree)
			CheckIfError(err)
			for _, element := range patch.FilePatches() {
				from, to := element.Files()
				var oldContent = ""
				var oldSize = 0
				var newContent = ""
				var newSize = 0
				if from != nil {
					blob, err := repo.BlobObject(from.Hash())
					oldSize = int(blob.Size)
					CheckIfError(err)
					if blob.Size < MAX_FILE_SIZE && !element.IsBinary() {
						reader, err := blob.Reader()
						CheckIfError(err)
						var data = make([]byte, blob.Size)
						_, err = reader.Read(data)
						// TODO check what's the cause here
						if err == nil {
							oldContent = string(data)
						}
					}
				}
				if to != nil {
					blob, err := repo.BlobObject(to.Hash())
					newSize = int(blob.Size)
					CheckIfError(err)
					if blob.Size < MAX_FILE_SIZE && !element.IsBinary() {
						reader, err := blob.Reader()
						CheckIfError(err)
						var data = make([]byte, blob.Size)
						_, err = reader.Read(data)
						// TODO check what's the cause here
						if err == nil {
							newContent = string(data)
						}
					}
				}
				var path = ""
				if to != nil {
					path = to.Path()
				} else {
					path = from.Path()
				}
				var nameElements = strings.Split(path, "/")
				var extension = getExtension(path)
				var dir0 = safeGet(nameElements, 0)
				var dir1 = safeGet(nameElements, 1)
				var dir2 = safeGet(nameElements, 2)
				var dir3 = safeGet(nameElements, 3)
				var dir4 = safeGet(nameElements, 4)
				var dir5 = safeGet(nameElements, 5)
				var dir6 = safeGet(nameElements, 6)
				var dir7 = safeGet(nameElements, 7)
				var dir8 = safeGet(nameElements, 8)
				var dir9 = safeGet(nameElements, 9)
				if from != nil && to != nil {
					var existingDescriptor = currentFileDescriptors[to.Path()]
					existingDescriptor.ValidWithin.Lte = formattedCommitDate
					var modifyRevision = FileRevisionDescriptor{
						Hash:        commit.Hash.String(),
						AuthorName:  commit.Author.Name,
						AuthorEmail: commit.Author.Email,
						Date:        formattedCommitDate,
						Message:     commit.Message,
						Branch:      branchname,
						Repository:  reponame,
						Size:        newSize,
						Loc:         len(strings.Split(newContent, "\n")),
						Extension:   extension,
						Dir0:        dir0,
						Dir1:        dir1,
						Dir2:        dir2,
						Dir3:        dir3,
						Dir4:        dir4,
						Dir5:        dir5,
						Dir6:        dir6,
						Dir7:        dir7,
						Dir8:        dir8,
						Dir9:        dir9,
						FileName:    to.Path(),
						OldPath:     from.Path(),
						Operation:   "modify",
						NewContent:  newContent,
						OldContent:  oldContent,
						ValidWithin: DateRange{
							Gte: formattedCommitDate,
							Lte: "",
						},
						OldSize:      oldSize,
						OldLoc:       len(strings.Split(oldContent, "\n")),
						LinesAdded:   getAddedLines(element),
						LinesRemoved: getRemovedLines(element),
					}
					// if to.Path() == "dist/rename-me.js" {
					// 	fmt.Println(modifyRevision)
					// }
					writeQueue <- existingDescriptor
					currentFileDescriptors[to.Path()] = modifyRevision
				}
				if from != nil && to == nil {
					var existingDescriptor = currentFileDescriptors[from.Path()]
					existingDescriptor.ValidWithin.Lte = formattedCommitDate
					var deleteRevision = FileRevisionDescriptor{
						Hash:         commit.Hash.String(),
						AuthorName:   commit.Author.Name,
						AuthorEmail:  commit.Author.Email,
						Date:         formattedCommitDate,
						Message:      commit.Message,
						Branch:       branchname,
						Repository:   reponame,
						Size:         0,
						Loc:          0,
						Extension:    extension,
						Dir0:         dir0,
						Dir1:         dir1,
						Dir2:         dir2,
						Dir3:         dir3,
						Dir4:         dir4,
						Dir5:         dir5,
						Dir6:         dir6,
						Dir7:         dir7,
						Dir8:         dir8,
						Dir9:         dir9,
						OldPath:      "",
						FileName:     from.Path(),
						Operation:    "remove",
						NewContent:   "",
						OldContent:   oldContent,
						OldSize:      oldSize,
						OldLoc:       len(strings.Split(oldContent, "\n")),
						LinesAdded:   []string{},
						LinesRemoved: []string{},
						ValidWithin: DateRange{
							Gte: formattedCommitDate,
							Lte: formattedCommitDate,
						},
					}
					writeQueue <- existingDescriptor
					writeQueue <- deleteRevision
					delete(currentFileDescriptors, from.Path())
				}
				if from == nil && to != nil {
					var fileDescriptor = FileRevisionDescriptor{
						Hash:        commit.Hash.String(),
						AuthorName:  commit.Author.Name,
						AuthorEmail: commit.Author.Email,
						Date:        formattedCommitDate,
						Message:     commit.Message,
						Branch:      branchname,
						Repository:  reponame,
						Size:        newSize,
						Loc:         len(strings.Split(newContent, "\n")),
						Extension:   extension,
						Dir0:        dir0,
						Dir1:        dir1,
						Dir2:        dir2,
						Dir3:        dir3,
						Dir4:        dir4,
						Dir5:        dir5,
						Dir6:        dir6,
						Dir7:        dir7,
						Dir8:        dir8,
						Dir9:        dir9,
						FileName:    to.Path(),
						Operation:   "add",
						NewContent:  newContent,
						OldContent:  "",
						ValidWithin: DateRange{
							Gte: formattedCommitDate,
							Lte: "",
						},
						OldSize:      0,
						OldLoc:       0,
						LinesAdded:   []string{},
						LinesRemoved: []string{},
					}

					currentFileDescriptors[to.Path()] = fileDescriptor
				}
			}
		}

		index--
	}

	for _, fileDescriptor := range currentFileDescriptors {
		writeQueue <- fileDescriptor
	}

	close(writeQueue)
	fmt.Println("Wait for ES")
	<-done
}
