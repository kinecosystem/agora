package cmd

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var exportFileRegexp = regexp.MustCompile(`(?P<type>creations|payments)-(?P<start>\d+)-(?P<end>\d+).json`)

func compact(_ *cobra.Command, args []string) error {
	stat, err := os.Stat(args[0])
	if err != nil {
		return err
	}
	if !stat.IsDir() {
		return errors.Errorf("%s is not a directory", args[0])
	}

	stat, err = os.Stat(args[1])
	if os.IsNotExist(err) {
		if err := os.Mkdir(args[1], 0777); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else if !stat.IsDir() {
		return errors.Errorf("%s is not a directory", args[0])
	}

	creations, payments, err := computeCompactionSets(args[0], compactionSize)
	if err != nil {
		return errors.Wrap(err, "failed to compute compaction sets")
	}

	for _, cs := range creations {
		log.WithField("compaction_set", cs).Debug("processing creation compaction set")
		out := filepath.Join(args[1], fmt.Sprintf("creations-%d-%d.json", cs.inputs[0].start, cs.inputs[len(cs.inputs)-1].end))
		var in []string
		for _, f := range cs.inputs {
			in = append(in, filepath.Join(args[0], fmt.Sprintf("creations-%d-%d.json", f.start, f.end)))
		}

		if err := compactSet(in, out); err != nil {
			return errors.Wrapf(err, "failed to compact: %s", out)
		}
	}
	for _, cs := range payments {
		log.WithField("compaction_set", cs).Debug("processing payment compaction set")
		out := filepath.Join(args[1], fmt.Sprintf("payments-%d-%d.json", cs.inputs[0].start, cs.inputs[len(cs.inputs)-1].end))
		var in []string
		for _, f := range cs.inputs {
			in = append(in, filepath.Join(args[0], fmt.Sprintf("payments-%d-%d.json", f.start, f.end)))
		}

		if err := compactSet(in, out); err != nil {
			return errors.Wrapf(err, "failed to compact: %s", out)
		}
	}

	log.WithFields(log.Fields{
		"creation_sets": len(creations),
		"payment_sets":  len(payments),
	}).Info("Complete")

	return nil
}

// given a set of files, reduce into a larger file over a given interval
type fileInfo struct {
	name  string
	start uint64
	end   uint64
}

type sortableFileInfo []fileInfo

func (s sortableFileInfo) Len() int               { return len(s) }
func (s sortableFileInfo) Less(i int, j int) bool { return s[i].start < s[j].start }
func (s sortableFileInfo) Swap(i int, j int)      { s[i], s[j] = s[j], s[i] }

type compactionSet struct {
	size   int64
	inputs []fileInfo
}

func computeCompactionSets(dir string, targetSize int64) (creations, payments []compactionSet, err error) {
	var creationFiles []fileInfo
	var paymentFiles []fileInfo

	err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		matches := exportFileRegexp.FindStringSubmatch(info.Name())
		if len(matches) != 4 {
			return nil
		}

		start, err := strconv.ParseUint(matches[2], 10, 64)
		if err != nil {
			return errors.Wrapf(err, "invalid export file: %s", info.Name())
		}
		end, err := strconv.ParseUint(matches[3], 10, 64)
		if err != nil {
			return errors.Wrapf(err, "invalid export file: %s", info.Name())
		}

		fInfo := fileInfo{
			name:  path,
			start: start,
			end:   end,
		}

		log.WithField("info", fInfo).Debug("Adding")

		switch matches[1] {
		case "creations":
			creationFiles = append(creationFiles, fInfo)
		case "payments":
			paymentFiles = append(paymentFiles, fInfo)
		}

		return nil
	})
	if err != nil {
		return
	}

	sort.Sort(sortableFileInfo(creationFiles))
	sort.Sort(sortableFileInfo(paymentFiles))

	var lastBlock uint64
	var cs compactionSet
	for i, c := range creationFiles {
		if i > 0 {
			if c.start != lastBlock {
				return nil, nil, errors.Errorf("gap in creation files (expected %d)", lastBlock)
			}
		}
		lastBlock = c.end

		cs.inputs = append(cs.inputs, c)
		info, err := os.Stat(c.name)
		if err != nil {
			return creations, payments, errors.Wrap(err, "failed to stat file")
		}
		cs.size += info.Size()

		if cs.size > targetSize {
			creations = append(creations, cs)
			cs = compactionSet{}
		}
	}
	if len(cs.inputs) > 0 {
		creations = append(creations, cs)
	}

	cs = compactionSet{}
	lastBlock = 0
	for i, p := range paymentFiles {
		if i > 0 {
			if p.start != lastBlock {
				return nil, nil, errors.Errorf("gap in creation files (expected %d)", lastBlock)
			}
		}
		lastBlock = p.end

		cs.inputs = append(cs.inputs, p)
		info, err := os.Stat(p.name)
		if err != nil {
			return creations, payments, errors.Wrap(err, "failed to stat file")
		}
		cs.size += info.Size()

		if cs.size > targetSize {
			payments = append(payments, cs)
			cs = compactionSet{}
		}
	}
	if len(cs.inputs) > 0 {
		payments = append(payments, cs)
	}

	return creations, payments, nil
}

func compactSet(in []string, out string) error {
	f, err := os.Create(out)
	if err != nil {
		return errors.Wrap(err, "failed to open file")
	}

	for _, name := range in {
		input, err := os.Open(name)
		if err != nil {
			return errors.Wrapf(err, "failed to open: %s", name)
		}

		if _, err := io.Copy(f, input); err != nil {
			return errors.Wrapf(err, "failed to copy %s", name)
		}
	}

	return nil
}
