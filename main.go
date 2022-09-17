package main

import (
	"fmt"
	"os"

	memfs "github.com/go-git/go-billy/v5/memfs"
	git "github.com/go-git/go-git/v5"
	plumbingObject "github.com/go-git/go-git/v5/plumbing/object"
	memory "github.com/go-git/go-git/v5/storage/memory"
)

func CheckIfError(err error) {
	if err == nil {
		return
	}

	fmt.Printf("\x1b[31;1m%s\x1b[0m\n", fmt.Sprintf("error: %s", err))
	os.Exit(1)
}

func main() {
	fs := memfs.New()

	repo, err := git.Clone(memory.NewStorage(), fs, &git.CloneOptions{
		URL: "file:///Users/joereuter/Clones/alpine",
	})
	CheckIfError(err)

	branch, err := repo.Branch("main")
	CheckIfError(err)

	ref, err := repo.Reference(branch.Merge, true)
	CheckIfError(err)

	commit, err := repo.CommitObject(ref.Hash())

	var commits []*plumbingObject.Commit
	for commit != nil {
		commits = append(commits, commit)
		commit, err = commit.Parent(0)
	}
	fmt.Printf("%v commits", len(commits))

	var index = len(commits) - 1
	for index >= 0 {
		var commit = commits[index]
		if index == (len(commits) - 1) {
			fmt.Println("----------")
			fmt.Println(commit.Hash.String())
			files, err := commit.Files()
			CheckIfError(err)
			files.ForEach(func(file *plumbingObject.File) error {
				fmt.Println(file.Name)
				return nil
			})
		} else {
			fmt.Println("----------")
			fmt.Println(commit.Hash.String())
			var prevCommit = commits[index+1]
			prevTree, err := prevCommit.Tree()
			tree, err := commit.Tree()
			CheckIfError(err)

			patch, err := tree.Patch(prevTree)
			CheckIfError(err)
			fmt.Println("Changed files")
			for _, element := range patch.FilePatches() {
				from, to := element.Files()
				if from != nil && to != nil {
					fmt.Println("Changed")
				}
				if from != nil && to == nil {
					fmt.Println("Deleted")
				}
				if from == nil && to != nil {
					fmt.Println("Added")
				}
				if from != nil {
					fmt.Println(from.Path())
				}
				if to != nil {
					fmt.Println(to.Path())
				}
			}
		}

		if index < len(commits)-10 {
			return
		}
		index--
	}
}
