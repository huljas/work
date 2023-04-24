package work

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"reflect"
)

// returns an error if the job fails, or there's a panic, or we couldn't reflect correctly.
// if we return an error, it signals we want the job to be retried.
func runJob(job *Job, ctxType reflect.Type, middleware []*middlewareHandler, jt *jobType) (returnCtx reflect.Value, returnError error) {
	log.Infof("### work.runJob() - running job %s, middlewares %d", job.Name, len(middleware))
	returnCtx = reflect.New(ctxType)
	currentMiddleware := 0
	maxMiddleware := len(middleware)

	var next NextMiddlewareFunc
	next = func() error {
		if currentMiddleware < maxMiddleware {
			mw := middleware[currentMiddleware]
			currentMiddleware++
			if mw.IsGeneric {
				err := mw.GenericMiddlewareHandler(job, next)
				if err != nil {
					log.Warnf("### work.runJob() - %s generic mw handler returned error: %s", job.Name, err)
				} else {
					log.Infof("### work.runJob() - %s generic mw handler ok", job.Name)
				}
				return err
			}
			res := mw.DynamicMiddleware.Call([]reflect.Value{returnCtx, reflect.ValueOf(job), reflect.ValueOf(next)})
			x := res[0].Interface()
			if x == nil {
				return nil
			}
			err := x.(error)
			return err
		}
		if jt.IsGeneric {
			err := jt.GenericHandler(job)
			if err != nil {
				log.Warnf("### work.runJob() - %s jt generic handler returned error: %s", job.Name, err)
			} else {
				log.Infof("### work.runJob() - %s jt generic handler ok", job.Name)
			}
			return err
		}
		res := jt.DynamicHandler.Call([]reflect.Value{returnCtx, reflect.ValueOf(job)})
		x := res[0].Interface()
		if x == nil {
			return nil
		}
		err := x.(error)
		return err
	}

	defer func() {
		if panicErr := recover(); panicErr != nil {
			// err turns out to be interface{}, of actual type "runtime.errorCString"
			// Luckily, the err sprints nicely via fmt.
			errorishError := fmt.Errorf("%v", panicErr)
			logError("runJob.panic", errorishError)
			returnError = errorishError
		}
	}()

	returnError = next()

	if returnError != nil {
		log.Warnf("### work.runJob() - %s returned error: %s", job.Name, returnError)
	} else {
		log.Infof("### work.runJob() - %s ok", job.Name)
	}

	return
}
