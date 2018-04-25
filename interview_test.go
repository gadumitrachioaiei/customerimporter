package customerimporter

import (
	"io"
	"os"
	"reflect"
	"strings"
	"testing"
)

var records = `
first_name,last_name,email,gender,ip_address
M,H,mh@github.io,Female,38.194.51.128
B,O,bo@cyberchimps.com,Female,197.54.209.129
D,H,dh@cyberchimps.com,Male,155.75.186.217
J,H,jh@github.io,Male,251.166.224.119
C,G,cg@statcounter.com,Male,57.171.52.110
E,R,er@rediff.com,Male,243.219.170.46
G,H,gh@acquirethisname.com,Male,30.97.220.14
D,H,dh@chicagocyberchimpstribune.com,Male,27.122.100.11
N,A,na@acquirethisname.com,Female,168.67.162.1
L,L,ll@blogtalkradio.com,Female,190.106.124.105
`

var expected = []Domain{
	{Name: "blogtalkradio.com", Count: 1},
	{Name: "chicagocyberchimpstribune.com", Count: 1},
	{Name: "rediff.com", Count: 1},
	{Name: "statcounter.com", Count: 1},
	{Name: "acquirethisname.com", Count: 2},
	{Name: "cyberchimps.com", Count: 2},
	{Name: "github.io", Count: 2},
}

func TestImportEmailDomain(t *testing.T) {
	// make some csv records and put them in an io reader
	rs := strings.TrimSpace(records)
	f := strings.NewReader(rs)
	// pass the io reader to our code and call it
	domains, err := ImportEmailDomain(f)
	// assert that it works correctly
	if err != nil {
		t.Fatalf("Importing domains error: %v", err)
	}
	if !reflect.DeepEqual(domains, expected) {
		t.Fatalf("Got emain domains\n%v\n, expected\n%v\n", domains, expected)
	}
}

func TestImportEmailDomainCustom(t *testing.T) {
	// pass the io reader to our code and call it
	f, err := os.Open("customers.csv")
	if err != nil {
		t.Fatalf("Opening input test file : %v", err)
	}
	defer f.Close()
	domains, err := ImportEmailDomain(f)
	// assert that it works correctly
	if err != nil {
		t.Fatalf("Importing domains error: %v", err)
	}
	// pass the io reader to our code and call it
	f.Seek(0, 0)
	domainsCustom, err := ImportEmailDomainCustom(f)
	// assert that it works correctly
	if err != nil {
		t.Fatalf("Importing domains custom error: %v", err)
	}
	if len(domains) != len(domainsCustom) {
		t.Fatalf("got domains: %d, got domains custom: %d, expected equality", len(domains), len(domainsCustom))
	}
	for i := range domains {
		if domains[i] != domainsCustom[i] {
			t.Fatalf("got domain: %v, got domain custom: %v, expected equality", domains[i], domainsCustom[i])
		}
	}
}

func TestImportEmailDomainConcurrent(t *testing.T) {
	// pass the io reader to our code and call it
	f, err := os.Open("customers.csv")
	if err != nil {
		t.Fatalf("Opening input test file : %v", err)
	}
	defer f.Close()
	domains, err := ImportEmailDomain(f)
	// assert that it works correctly
	if err != nil {
		t.Fatalf("Importing domains error: %v", err)
	}
	// pass the io reader to our code and call it
	f.Seek(0, 0)
	domainsConcurrent, err := ImportEmailDomainConcurrent(f.Name())
	// assert that it works correctly
	if err != nil {
		t.Fatalf("Importing domains conccurent error: %v", err)
	}
	if len(domains) != len(domainsConcurrent) {
		t.Fatalf("got domains: %d, got domains conccurent: %d, expected equality", len(domains), len(domainsConcurrent))
	}
	for i := range domains {
		if domains[i] != domainsConcurrent[i] {
			t.Fatalf("got domain: %v, got domain conccurent: %v, expected equality: %d", domains[i], domainsConcurrent[i], i)
		}
	}
}

var domains []Domain

// BenchmarkImportEmaiDomain/ImportEmailDomain-8         	    2000	   1100555 ns/op	  268406 B/op	    3047 allocs/op
// BenchmarkImportEmaiDomain/ImportEmailDomainCustom-8   	    2000	    737865 ns/op	  133662 B/op	    3035 allocs/op
func BenchmarkImportEmaiDomain(b *testing.B) {
	var run = func(b *testing.B, tested func(io.Reader) ([]Domain, error)) {
		var ds []Domain
		for n := 0; n < b.N; n++ {
			b.StopTimer()
			f, err := os.Open("customers.csv")
			if err != nil {
				b.Fatalf("Opening input test file : %v", err)
			}
			b.StartTimer()
			ds, err = tested(f)
			if err != nil {
				f.Close()
				b.Fatalf("Importing domains error: %v", err)
			}
			f.Close()
		}
		domains = ds
	}
	b.Run("ImportEmailDomain", func(b *testing.B) { run(b, ImportEmailDomain) })
	b.Run("ImportEmailDomainCustom", func(b *testing.B) { run(b, ImportEmailDomainCustom) })
}

// BenchmarkImportEmailDomainConcurrent-8   	     300	   5656359 ns/op	  592034 B/op	    3245 allocs/op
func BenchmarkImportEmailDomainConcurrent(b *testing.B) {
	var ds []Domain
	var err error
	for n := 0; n < b.N; n++ {
		ds, err = ImportEmailDomainConcurrent("customers.csv")
		if err != nil {
			b.Fatal(err)
		}
	}
	domains = ds
}
