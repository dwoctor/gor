package gor

import "github.com/fzzy/radix/redis"

// A redis database.
type Database struct {
	address string
}

// Creates a new redis database.
func NewDatabase(address string) *Collection {
	return &Server{address: address}
}

// Puts a key/value into the database.
func (this *Database) Put(key *string, value *[]byte) error {
	client, err := redis.Dial("tcp", this.address)
	if err != nil {
		return err
	}
	defer client.Close()
	reply := client.Cmd("SET", key, value)
	if reply.Err != nil {
		return reply.Err
	}
	return nil
}

// Gets a key/value into the database.
func (this *Database) Get(key *string) (*[]byte, error) {
	client, err := redis.Dial("tcp", this.address)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	data, err := client.Cmd("GET", key).Bytes()
	if err != nil {
		return nil, err
	}
	return &data, nil
}

// Does a key/value in the database exist.
func (this *Database) Has(key *string) (bool, error) {
	client, err := redis.Dial("tcp", this.address)
	if err != nil {
		return false, err
	}
	defer client.Close()
	if found, err := client.Cmd("EXISTS", key).Int(); err != nil {
		return false, err
	} else if found != 1 {
		return false, nil
	} else {
		return true, nil
	}
}

// Perfoms a Has and Get operation.
func (this *Database) Fetch(key *string) (*[]byte, error) {
	if found, err := FindUserFromRedis(key); err != nil {
		return nil, err
	} else if found == false {
		return nil, nil
	} else {
		return this.LoadFromRedis(key)
	}
}

// Clears the database.
func (this *Database) Clear() error {
	client, err := redis.Dial("tcp", this.address)
	if err != nil {
		return err
	}
	defer client.Close()
	if err := client.Cmd("FLUSHDB").Err; err != nil {
		return err
	}
	if err := client.Cmd("FLUSHALL").Err; err != nil {
		return err
	}
	return nil
}
