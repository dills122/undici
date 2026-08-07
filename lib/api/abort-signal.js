'use strict'

const { addAbortListener } = require('../core/util')
const { RequestAbortedError } = require('../core/errors')

const kListener = Symbol('kListener')
const kSignal = Symbol('kSignal')

function abort (self) {
  self.aborted = true
  self.reason = self[kSignal]?.reason ?? new RequestAbortedError()
  if (self.abort) {
    self.abort(self.reason)
  }
  removeSignal(self)
}

function addSignal (self, signal) {
  self.aborted = false
  self.reason = null

  self[kSignal] = null
  self[kListener] = null

  if (!signal) {
    return
  }

  if (signal.aborted) {
    self.aborted = true
    self.reason = signal.reason ?? new RequestAbortedError()
    return
  }

  self[kSignal] = signal
  self[kListener] = () => {
    abort(self)
  }

  addAbortListener(self[kSignal], self[kListener])
}

function removeSignal (self) {
  if (!self[kSignal]) {
    return
  }

  if ('removeEventListener' in self[kSignal]) {
    self[kSignal].removeEventListener('abort', self[kListener])
  } else {
    self[kSignal].removeListener('abort', self[kListener])
  }

  self[kSignal] = null
  self[kListener] = null
}

function setAbort (self, abortRequest) {
  if (self.aborted) {
    abortRequest(self.reason)
  } else {
    self.abort = abortRequest
  }
}

module.exports = {
  addSignal,
  removeSignal,
  setAbort
}
