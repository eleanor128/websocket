package main

import (
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
)

// Optional diagnostic profiling. Profiles include replay and result export;
// use unprofiled runs for reported latency comparisons.
func profiledRun(o options, directory string) error {
	if directory == "" {
		return run(o)
	}
	if err := os.Mkdir(directory, 0700); err != nil {
		return err
	}
	f, err := os.Create(filepath.Join(directory, "cpu.pprof"))
	if err != nil {
		return err
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		f.Close()
		return err
	}
	old := runtime.SetMutexProfileFraction(10)
	defer func() {
		pprof.StopCPUProfile()
		f.Close()
		runtime.SetMutexProfileFraction(old)
		for _, name := range []string{"mutex", "heap"} {
			if out, err := os.Create(filepath.Join(directory, name+".pprof")); err == nil {
				pprof.Lookup(name).WriteTo(out, 0)
				out.Close()
			}
		}
	}()
	return run(o)
}
