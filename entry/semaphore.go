package entry

type empty struct{}
type Semaphore chan empty

// acquire n resources
func (s Semaphore) P(n int) {
	e := empty{}
	for i := 0; i < n; i++ {
		s <- e
	}
}

// release n resources
func (s Semaphore) V(n int) {
	for i := 0; i < n; i++ {
		<-s
	}
}

/* mutexes */
func (s Semaphore) Lock() {
	s.P(1)
}

func (s Semaphore) Unlock() {
	s.V(1)
}

/* signal-wait */
func (s Semaphore) Signal() {
	s.V(1)
}

func (s Semaphore) Wait(n int) {
	s.P(n)
}
