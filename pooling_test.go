package dbox

import (
	"testing"
	"time"

	"github.com/eaciit/toolkit"

	. "github.com/smartystreets/goconvey/convey"
)

type fakeConnection struct {
	ConnectionBase
}

func (c *fakeConnection) Connect() error {
	return nil
}

func (c *fakeConnection) Close() {
}

func TestDbPooling(t *testing.T) {
	Convey("DB Pooling", t, func() {
		p := NewDbPooling(3, func() (IConnection, error) {
			conn := new(fakeConnection)
			conn.Connect()
			return conn, nil
		})

		Convey("Initiate connections", func() {
			pi1, err1 := p.Get()
			pi2, err2 := p.Get()

			Convey("Connection are generated", func() {
				So(pi1, ShouldNotBeNil)
				So(pi2, ShouldNotBeNil)
			})

			Convey("Error are nil", func() {
				So(err1, ShouldBeNil)
				So(err2, ShouldBeNil)
			})

			Convey("Count should be 2", func() {
				So(p.Count(), ShouldEqual, 2)
			})

			Convey("Make more connection", func() {
				p.Get()
				Convey("Count should be 3", func() {
					So(p.Count(), ShouldEqual, 3)
				})

				Convey("Make 4th connection, should be rejected", func() {
					_, err := p.Get()
					So(err, ShouldNotBeNil)
					toolkit.Printfn("\nErr: %s", err.Error())

					Convey("Count should be still 3", func() {
						So(p.Count(), ShouldEqual, 3)
					})
				})

				Convey("Release a connection", func() {
					pi1.Release()

					Convey("Err should be nil", func() {
						_, err := p.Get()
						So(err, ShouldBeNil)

						Convey("Count remains 3", func() {
							So(p.Count(), ShouldEqual, 3)

							p.Close()
						})
					})
				})
			})
		})
	})
}

func TestPoolingQue(t *testing.T) {
	Convey("Test Que", t, func() {
		p := NewDbPooling(3, func() (IConnection, error) {
			conn := new(fakeConnection)
			conn.Connect()
			return conn, nil
		})

		Convey("Prepare 3 connections", func() {
			p1, err1 := p.Get()
			_, err2 := p.Get()
			_, err3 := p.Get()

			So(err1, ShouldBeNil)
			So(err2, ShouldBeNil)
			So(err3, ShouldBeNil)

			Convey("Get 4th connection and it should wait for 1s", func() {
				go func() {
					time.Sleep(1 * time.Second)
					p1.Release()
				}()

				_, err2 := p.Get()
				So(err2, ShouldBeNil)

				p.Close()
			})
		})
	})
}

func TestAutorelease(t *testing.T) {
	Convey("Test Autorelease", t, func() {
		p := NewDbPooling(3, func() (IConnection, error) {
			conn := new(fakeConnection)
			conn.Connect()
			return conn, nil
		})
		p.AutoRelease = 1 * time.Second

		Convey("Prepare 3 connections and hold them for 1.2s", func() {
			_, err1 := p.Get()
			_, err2 := p.Get()
			_, err3 := p.Get()

			So(err1, ShouldBeNil)
			So(err2, ShouldBeNil)
			So(err3, ShouldBeNil)

			time.Sleep(1200 * time.Millisecond)

			Convey("All of 3 connection should be release already", func() {
				So(p.FreeCount(), ShouldEqual, 3)
			})
		})
	})
}

func TestAutoClose(t *testing.T) {
	Convey("Test Auto Close", t, func() {
		p := NewDbPooling(3, func() (IConnection, error) {
			conn := new(fakeConnection)
			conn.Connect()
			return conn, nil
		})

		Convey("Prepare 3 connections and hold them for 1.2s while set autoclose of first one to 1s", func() {
			p1, err1 := p.Get()
			p1.AutoClose = 1 * time.Second

			_, err2 := p.Get()
			_, err3 := p.Get()

			So(err1, ShouldBeNil)
			So(err2, ShouldBeNil)
			So(err3, ShouldBeNil)

			go func() {
				time.Sleep(1 * time.Second)
				p1.Release()
			}()

			time.Sleep(2200 * time.Millisecond)

			Convey("1 conn should be closed. 2 are not yet", func() {
				So(p.ClosedCount(), ShouldEqual, 1)
				So(p.FreeCount(), ShouldEqual, 0)
			})
		})
	})
}
