package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/jcelliott/lumber"
)

const Version = "1.0.0"

type (
	Logger interface {
		Fatal(string, ...interface{})
		Error(string, ...interface{})
		Warn(string, ...interface{})
		Info(string, ...interface{})
		Debug(string, ...interface{})
		Trace(string, ...interface{})
	}

	Driver struct {
		mutex   sync.Mutex
		mutexes map[string]*sync.Mutex
		dir     string
		log     Logger
	}

	Options struct {
		Logger
	}
)

func New(dir string, options *Options) (*Driver, error) {
	dir = filepath.Clean(dir)

	opts := &Options{}
	if options != nil {
		opts = options
	}

	if opts.Logger == nil {
		opts.Logger = lumber.NewConsoleLogger((lumber.INFO))
	}

	driver := Driver{
		dir:     dir,
		mutexes: make(map[string]*sync.Mutex),
		log:     opts.Logger,
	}

	if _, err := os.Stat(dir); err == nil {
		opts.Logger.Debug("Using '%s' ('database already exists') \n", dir)
		return &driver, nil
	}

	opts.Logger.Debug("Creating Database at '%s'...  \n", dir)
	return &driver, os.Mkdir(dir, 0755) // 0755 is the read access permission
}

func (d *Driver) Write(collection string, resource string, v interface{}) error {
	err := validateCollectionResource(collection, resource)
	if err != nil {
		return err
	}

	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, collection)
	fnlPath := filepath.Join(dir, resource+".json")
	tmpPath := fnlPath + ".tmp"

	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	b, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return err
	}

	b = append(b, byte('\n'))

	if err := os.WriteFile(tmpPath, b, 0644); err != nil {
		return err
	}

	return os.Rename(tmpPath, fnlPath)
}

func (d *Driver) Read(collection string, resource string, v interface{}) error {
	err := validateCollectionResource(collection, resource)
	if err != nil {
		return err
	}

	record := filepath.Join(d.dir, collection, resource)

	if _, err := stat(record); err != nil {
		return err
	}

	b, err := os.ReadFile(record + ".json")
	if err != nil {
		return err
	}

	return json.Unmarshal(b, &v)
}

func validateCollectionResource(collection string, resource string) error {
	if collection == "" {
		return errors.New("Missing Collection - no place to save the records!")
	}

	if resource == "" {
		return errors.New("Missing Resource - unable to save record (No Name)!")
	}

	return nil
}

func (d *Driver) ReadAll(collection string) ([]string, error) {
	if collection == "" {
		return nil, errors.New("Missing Collection - unable to read!")
	}

	dir := filepath.Join(d.dir, collection)

	if _, err := stat(dir); err != nil {
		return nil, err
	}

	files, _ := os.ReadDir(dir)

	var records []string

	for _, f := range files {
		b, err := os.ReadFile(filepath.Join(dir, f.Name()))
		if err != nil {
			return nil, err
		}

		records = append(records, string(b))
	}

	return records, nil
}

func (d *Driver) Delete(collection string, resource string) error {
	err := validateCollectionResource(collection, resource)
	if err != nil {
		return err
	}

	path := filepath.Join(collection, resource)
	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, path)
	switch fi, err := stat(dir); {
	case fi == nil, err != nil:
		return fmt.Errorf("Unable to find record. Dir %v\n", path)
	case fi.Mode().IsDir():
		return os.RemoveAll(dir)

	case fi.Mode().IsRegular():
		return os.RemoveAll(dir + ".json")
	}

	return nil
}

func (d *Driver) getOrCreateMutex(collection string) *sync.Mutex {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	m, ok := d.mutexes[collection]

	if !ok {
		m = &sync.Mutex{}
		d.mutexes[collection] = m
	}

	return m
}

func stat(path string) (fi os.FileInfo, err error) {
	if fi, err = os.Stat(path); os.IsNotExist(err) {
		fi, err = os.Stat(path + ".json")
	}
	return
}

type Address struct {
	City    string
	State   string
	Country string
	Pincode json.Number
}

type User struct {
	Name    string
	Age     json.Number
	Contact string
	Company string
	Address Address
}

func main() {
	dir := "./"

	db, err := New(dir, nil)
	if err != nil {
		fmt.Println(err)
	}

	employees := []User{
		{"Thrillee", "22", "2348154396918", "Thrillee Tech", Address{"Ikeja", "Lagos", "9ja", "12345"}},
		{"John Doe", "19", "2348154397777", "Saas Tech", Address{"Ikorodu", "Lagos", "9ja", "88845"}},
		{"Albert Doe", "89", "2348154397887", "Google Tech", Address{"Egbeda", "Lagos", "9ja", "88845"}},
	}

	for _, e := range employees {
		// Write employee
		db.Write("user", e.Name, User{
			Name:    e.Name,
			Age:     e.Age,
			Contact: e.Contact,
			Company: e.Company,
			Address: Address{
				City:    e.Address.City,
				State:   e.Address.State,
				Country: e.Address.Country,
				Pincode: e.Address.Pincode,
			},
		})
	}

	records, err := db.ReadAll("user")
	if err != nil {
		fmt.Println(err)
	}

	allUsers := []User{}
	for _, f := range records {
		empFound := User{}
		if err := json.Unmarshal([]byte(f), &empFound); err != nil {
			fmt.Println("Error", err)
		}

		allUsers = append(allUsers, empFound)
	}

	fmt.Println(">>>>>>>>>>>>>>>>READING RECORDS<<<<<<<<<<<<<<<<")
	fmt.Println(allUsers)
}
