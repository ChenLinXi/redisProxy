package module

/*
	readChan & writeChan for go routine
 */

type Buffer struct {
	rc	chan []byte
	wc	chan []byte
}
