package dht

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	key "github.com/ipfs/go-ipfs/blocks/key"
	peer "gx/ipfs/QmQGwpJy9P4yXZySmqkZEXCmbBpJUb8xntCv8Ca4taZwDC/go-libp2p-peer"
	goprocess "gx/ipfs/QmQopLATEYMNg7dVqZRNDfeE2S1yKy8zrRh5xnYiuqeZBn/goprocess"
	goprocessctx "gx/ipfs/QmQopLATEYMNg7dVqZRNDfeE2S1yKy8zrRh5xnYiuqeZBn/goprocess/context"
	lru "gx/ipfs/QmVYxfoJQiZijTgPNHCHgHELvQpbsJNTg6Crmc3dQkj3yy/golang-lru"
	ds "gx/ipfs/QmZ6A6P6AMo8SR3jXAwzTuSU6B9R2Y4eqW2yW9VvfUayDN/go-datastore"
	dsq "gx/ipfs/QmZ6A6P6AMo8SR3jXAwzTuSU6B9R2Y4eqW2yW9VvfUayDN/go-datastore/query"

	context "gx/ipfs/QmZy2y8t9zQH2a1b8q2ZSLKp17ATuJoCNxxyMFG5qFExpt/go-net/context"
)

var lruCacheSize = 256
var ProvideValidity = time.Hour * 24
var defaultCleanupInterval = time.Hour

type ProviderManager struct {
	// all non channel fields are meant to be accessed only within
	// the run method
	providers *lru.Cache
	local     map[key.Key]struct{}
	lpeer     peer.ID
	dstore    ds.Datastore

	getlocal chan chan []key.Key
	newprovs chan *addProv
	getprovs chan *getProv
	period   time.Duration
	proc     goprocess.Process

	cleanupInterval time.Duration
}

type providerSet struct {
	providers []peer.ID
	set       map[peer.ID]time.Time
}

type addProv struct {
	k   key.Key
	val peer.ID
}

type getProv struct {
	k    key.Key
	resp chan []peer.ID
}

func NewProviderManager(ctx context.Context, local peer.ID, dstore ds.Datastore) *ProviderManager {
	pm := new(ProviderManager)
	pm.getprovs = make(chan *getProv)
	pm.newprovs = make(chan *addProv)
	pm.dstore = dstore
	cache, err := lru.New(lruCacheSize)
	if err != nil {
		panic(err) //only happens if negative value is passed to lru constructor
	}
	pm.providers = cache

	pm.getlocal = make(chan chan []key.Key)
	pm.local = make(map[key.Key]struct{})
	pm.proc = goprocessctx.WithContext(ctx)
	pm.cleanupInterval = defaultCleanupInterval
	pm.proc.Go(func(p goprocess.Process) { pm.run() })

	return pm
}

const providersKeyPrefix = "/providers/"

func mkProvKey(k key.Key) ds.Key {
	return ds.NewKey(providersKeyPrefix + string(k))
}

func (pm *ProviderManager) getProvs(k key.Key) ([]peer.ID, error) {
	pset, err := pm.getPset(k)
	if err != nil {
		return nil, err
	}
	return pset.providers, nil
}

func (pm *ProviderManager) getPset(k key.Key) (*providerSet, error) {
	iprovs, ok := pm.providers.Get(k)
	if ok {
		provs := iprovs.(*providerSet)
		return provs, nil
	}
	pset, err := pm.loadProvSet(k)
	if err != nil {
		return nil, err
	}
	pm.providers.Add(k, pset)
	return pset, nil
}

func (pm *ProviderManager) addProv(k key.Key, p peer.ID) error {
	iprovs, ok := pm.providers.Get(k)
	if !ok {
		iprovs = newProviderSet()
		pm.providers.Add(k, iprovs)
	}
	provs := iprovs.(*providerSet)
	provs.Add(p)

	return pm.storeProvSet(k, provs)
}

func (pm *ProviderManager) storeProvSet(k key.Key, pset *providerSet) error {
	buf := new(bytes.Buffer)
	_, err := pset.WriteTo(buf)
	if err != nil {
		return err
	}

	return pm.dstore.Put(mkProvKey(k), buf.Bytes())
}

func (pm *ProviderManager) loadProvSet(k key.Key) (*providerSet, error) {
	val, err := pm.dstore.Get(mkProvKey(k))
	if err != nil {
		return nil, err
	}

	valb, ok := val.([]byte)
	if !ok {
		log.Errorf("value for providers set was not bytes. (instead got %#v)", val)
		return nil, fmt.Errorf("value was not bytes!")
	}

	pset, err := psetFromReader(bytes.NewReader(valb))
	if err != nil {
		return nil, err
	}

	return pset, nil
}

func (pm *ProviderManager) deleteProvSet(k key.Key) error {
	pm.providers.Remove(k)

	return pm.dstore.Delete(mkProvKey(k))
}

func (pm *ProviderManager) getAllProvKeys() ([]key.Key, error) {
	res, err := pm.dstore.Query(dsq.Query{
		KeysOnly: true,
		Prefix:   providersKeyPrefix,
	})

	if err != nil {
		return nil, err
	}

	entries, err := res.Rest()
	if err != nil {
		return nil, err
	}

	out := make([]key.Key, 0, len(entries))
	for _, e := range entries {
		prov := key.Key(e.Key[len(providersKeyPrefix):])
		out = append(out, prov)
	}

	return out, nil
}

func (pm *ProviderManager) run() {
	tick := time.NewTicker(pm.cleanupInterval)
	for {
		select {
		case np := <-pm.newprovs:
			if np.val == pm.lpeer {
				pm.local[np.k] = struct{}{}
			}
			err := pm.addProv(np.k, np.val)
			if err != nil {
				log.Error("error adding new providers: ", err)
			}
		case gp := <-pm.getprovs:
			provs, err := pm.getProvs(gp.k)
			if err != nil && err != ds.ErrNotFound {
				log.Error("error reading providers: ", err)
			}

			gp.resp <- provs
		case lc := <-pm.getlocal:
			var keys []key.Key
			for k := range pm.local {
				keys = append(keys, k)
			}
			lc <- keys

		case <-tick.C:
			keys, err := pm.getAllProvKeys()
			if err != nil {
				log.Error("Error loading provider keys: ", err)
				continue
			}
			for _, k := range keys {
				provs, err := pm.getPset(k)
				if err != nil {
					log.Error("error loading known provset: ", err)
					continue
				}
				var filtered []peer.ID
				for p, t := range provs.set {
					if time.Now().Sub(t) > ProvideValidity {
						delete(provs.set, p)
					} else {
						filtered = append(filtered, p)
					}
				}

				if len(filtered) > 0 {
					provs.providers = filtered
				} else {
					err := pm.deleteProvSet(k)
					if err != nil {
						log.Error("error deleting provider set: ", err)
					}
				}
			}
		case <-pm.proc.Closing():
			return
		}
	}
}

func (pm *ProviderManager) AddProvider(ctx context.Context, k key.Key, val peer.ID) {
	prov := &addProv{
		k:   k,
		val: val,
	}
	select {
	case pm.newprovs <- prov:
	case <-ctx.Done():
	}
}

func (pm *ProviderManager) GetProviders(ctx context.Context, k key.Key) []peer.ID {
	gp := &getProv{
		k:    k,
		resp: make(chan []peer.ID, 1), // buffered to prevent sender from blocking
	}
	select {
	case <-ctx.Done():
		return nil
	case pm.getprovs <- gp:
	}
	select {
	case <-ctx.Done():
		return nil
	case peers := <-gp.resp:
		return peers
	}
}

func (pm *ProviderManager) GetLocal() []key.Key {
	resp := make(chan []key.Key)
	pm.getlocal <- resp
	return <-resp
}

func newProviderSet() *providerSet {
	return &providerSet{
		set: make(map[peer.ID]time.Time),
	}
}

func (ps *providerSet) Add(p peer.ID) {
	_, found := ps.set[p]
	if !found {
		ps.providers = append(ps.providers, p)
	}

	ps.set[p] = time.Now()
}

func psetFromReader(r io.Reader) (*providerSet, error) {
	br, ok := r.(io.ByteReader)
	if !ok {
		bufr := bufio.NewReader(r)
		br = bufr
		r = bufr
	}

	out := newProviderSet()
	buf := make([]byte, 128)
	for {
		v, err := binary.ReadUvarint(br)
		if err != nil {
			if err == io.EOF {
				return out, nil
			}
			return nil, err
		}

		_, err = io.ReadFull(r, buf[:v])
		if err != nil {
			if err == io.EOF {
				return out, nil
			}
			return nil, err
		}

		pid := peer.ID(buf[:v])

		tnsec, err := binary.ReadVarint(br)
		if err != nil {
			if err == io.EOF {
				return out, nil
			}
			return nil, err
		}

		t := time.Unix(0, tnsec)
		out.set[pid] = t
		out.providers = append(out.providers, pid)
	}
}

func (ps *providerSet) WriteTo(w io.Writer) (int64, error) {
	var written int64
	buf := make([]byte, 16)
	for _, p := range ps.providers {
		t := ps.set[p]
		id := []byte(p)
		n := binary.PutUvarint(buf, uint64(len(id)))
		_, err := w.Write(buf[:n])
		if err != nil {
			return written, err
		}
		written += int64(n)
		n, err = w.Write(id)
		if err != nil {
			return written, err
		}

		written += int64(n)
		n = binary.PutVarint(buf, t.UnixNano())
		_, err = w.Write(buf[:n])
		if err != nil {
			return written, err
		}
	}

	return written, nil
}
