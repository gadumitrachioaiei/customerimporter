// Package customerimporter reads from the given customers.csv file and returns a
// sorted (data structure of your choice) of email domains along with the number
// of customers with e-mail addresses for each domain.  Any errors should be
// logged (or handled). Performance matters (this is only ~3k lines, but *could*
// be 1m lines or run on a small machine).
package customerimporter

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"io"
	"os"
	"sort"
	"sync/atomic"

	"github.com/pkg/errors"
)

type Domain struct {
	Name  string
	Count int64
}

// ImportEmailDomain produces a list of email domains with number of emails for that domain
// it is the caller responsibility to close any open files
func ImportEmailDomain(in io.Reader) ([]Domain, error) {
	domainsMap, err := importEmailDomain(in, -1)
	if err != nil {
		return nil, err
	}
	return getDomains(domainsMap), nil
}

// ImportEmailDomainCustom is like ImportEmailDomain but uses a custom csv parser
func ImportEmailDomainCustom(in io.Reader) ([]Domain, error) {
	domainsMap, err := importEmailDomainCustom(in, -1)
	if err != nil {
		return nil, err
	}
	return getDomains(domainsMap), nil
}

// ImportEmailDomainConcurrent is like ImportEmailDomain but it does work concurrently
// it receives a path to a file, as it needs to seek lines in the file
func ImportEmailDomainConcurrent(path string) ([]Domain, error) {
	// we split the input, say process 1000 lines, maximum of 8 threads
	linesPerThreadWork := 1000
	threads := 8
	emailFieldIndex := -1
	// a simple pool
	work := make(chan io.ReadCloser, threads)
	var jobsCount int32
	done := make(chan struct{})
	terminate := make(chan struct{})
	errSignal := make(chan error)
	domainsMapList := make([]map[string]int64, 8)
	for i := 0; i < len(domainsMapList); i++ {
		domainsMapList[i] = make(map[string]int64)
	}
	for i := 0; i < threads; i++ {
		// a goroutine performs the given work, and store the data and signal any error
		// if error occurs the goroutine is still ready to perform other work
		go func(domainMap map[string]int64) {
			for {
				select {
				case <-terminate:
					return
				case in := <-work:
					domains, err := importEmailDomain(in, emailFieldIndex)
					in.Close()
					if err != nil {
						errSignal <- err
						break
					}
					for name := range domains {
						domainMap[name] += domains[name]
					}
				}
				if atomic.AddInt32(&jobsCount, -1) == 0 {
					close(done)
				}
			}
		}(domainsMapList[i])
	}
	in, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	// find the offset of each 1000 lines in the original file
	partsIndexes := make([]int64, 1)
	partsIndexes[0] = -1
	var newLineIndex int64
	newLines := 0
	// TODO: if there is a line smaller than 20 bytes this might not work
	p := make([]byte, 20)
	for {
		n, err := in.Read(p)
		if i := bytes.IndexByte(p[:n], '\n'); i >= 0 {
			newLines++
			if newLines == 1 && emailFieldIndex == -1 {
				// we want to skip the first line, which should be considered to be the header
				emailFieldIndex = 2 // TODO: calculate this from the header line
				partsIndexes[0] = newLineIndex + int64(i)
			} else if newLines == linesPerThreadWork {
				newLines = 0
				partsIndexes = append(partsIndexes, newLineIndex+int64(i))
			}
		}
		newLineIndex += int64(n)
		if err == nil {
			continue
		}
		if err == io.EOF {
			break
		}
		return nil, errors.Errorf("Reading: %v", err)
	}
	if newLines > 0 {
		partsIndexes = append(partsIndexes, newLineIndex)
	}
	// set the number of jobs that need handling
	atomic.StoreInt32(&jobsCount, int32(len(partsIndexes)-1))
	// in a new goroutine send work to our pool
	go func() {
		in.Seek(0, io.SeekStart)
		defer in.Close()
		for i := 0; i < len(partsIndexes)-1; i++ {
			var f *os.File
			var err error
			if f, err = os.Open(path); err != nil {
				errSignal <- err
				return
			}
			if _, err := f.Seek(partsIndexes[i]+1, io.SeekStart); err != nil {
				errSignal <- errors.Errorf("Seeking %s %d %v", path, partsIndexes[i], err)
				return
			}
			select {
			case work <- struct {
				io.Closer
				io.Reader
			}{f, io.LimitReader(f, partsIndexes[i+1]-partsIndexes[i]-1)}:
			case <-terminate:
				f.Close()
			}
		}
	}()
	// wait for all work to finish or for any errors
	select {
	case err := <-errSignal:
		close(terminate)
		return nil, errors.Errorf("Process file: %v", err)
	case <-done:
		// when the pool has finished work, terminate pool
		close(terminate)
	}
	// final work
	if len(domainsMapList) == 0 {
		return nil, nil
	}
	domainsMap := domainsMapList[0]
	for i := 1; i < len(domainsMapList); i++ {
		for name, count := range domainsMapList[i] {
			domainsMap[name] += count
		}
	}
	return getDomains(domainsMap), nil
}

// getDomains transforms map of domains to sorted list of domains
func getDomains(domainsMap map[string]int64) []Domain {
	var domains []Domain
	for name, count := range domainsMap {
		domains = append(domains, Domain{Name: name, Count: count})
	}
	sort.Slice(domains, func(i, j int) bool {
		if domains[i].Count == domains[j].Count {
			return domains[i].Name < domains[j].Name
		}
		return domains[i].Count < domains[j].Count
	})
	return domains
}

func importEmailDomain(in io.Reader, emailFieldIndex int) (map[string]int64, error) {
	r := csv.NewReader(in)
	r.ReuseRecord = true
	r.TrimLeadingSpace = true
	domainsMap := make(map[string]int64)
	for recordCount := 0; ; recordCount++ {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, errors.Errorf("Reading csv record: %v %v", err, record)
		}
		if emailFieldIndex == -1 && recordCount == 0 {
			for i := 0; i < len(record); i++ {
				if record[i] == "email" {
					emailFieldIndex = i
					break
				}
			}
			continue
		}
		if emailFieldIndex == -1 {
			return nil, errors.New("Cannot find email field in csv records")
		}
		email := record[emailFieldIndex]
		var emailDomain string
		// extract the email domain
		for i := 0; i < len(email); i++ {
			if email[i] == '@' {
				emailDomain = email[i+1:]
				break
			}
		}
		domainsMap[emailDomain]++
	}
	// convert to the final list
	return domainsMap, nil
}

func importEmailDomainCustom(in io.Reader, emailFieldIndex int) (map[string]int64, error) {
	domainsMap := make(map[string]int64)
	scanner := bufio.NewScanner(in)
	for i := 0; scanner.Scan(); i++ {
		data := scanner.Bytes()
		// find which field is the email, column names are in first line
		if emailFieldIndex == -1 && i == 0 {
			parts := bytes.Split(data, []byte{','})
			for i := range parts {
				if bytes.Equal(parts[i], []byte("email")) {
					emailFieldIndex = i
					break
				}
			}
			if emailFieldIndex == -1 {
				return nil, errors.New("Cannot find email field in csv records")
			}
			continue
		}
		// determine the email
		var email []byte
		commasCount := 0
		emailStart := -1
		for i, c := range data {
			if c != ',' {
				continue
			}
			commasCount++
			if commasCount == emailFieldIndex {
				emailStart = i
				continue
			} else if commasCount == emailFieldIndex+1 {
				email = data[emailStart+1 : i]
				break
			}
		}
		if len(email) == 0 || emailStart == -1 {
			return nil, errors.New("Cannot find email field in csv records")
		}
		if len(email) == 0 {
			email = data[emailStart:]
		}
		// extract the email domain
		var emailDomain []byte
		for i := 0; i < len(email); i++ {
			if email[i] == '@' {
				emailDomain = email[i+1:]
				break
			}
		}
		domainsMap[string(emailDomain)]++
	}
	if err := scanner.Err(); err != nil {
		return nil, errors.Errorf("Scanning input: %v", err)
	}
	return domainsMap, nil
}
