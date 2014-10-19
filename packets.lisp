;; -*- external-format: utf-8; -*-

;;; MySQL native driver for Lispworks
;;; ver: 20130529

;;; Based on http://dev.mysql.com/doc/internals/en/client-server-protocol.html

;;; Copyright (c) 2009-2013, Art Obrezan
;;; All rights reserved.
;;;
;;; Redistribution and use in source and binary forms, with or without
;;; modification, are permitted provided that the following conditions are met:
;;;
;;; 1. Redistributions of source code must retain the above copyright
;;;    notice, this list of conditions and the following disclaimer.
;;; 2. Redistributions in binary form must reproduce the above copyright
;;;    notice, this list of conditions and the following disclaimer in the
;;;    documentation and/or other materials provided with the distribution.
;;; 3. Use in source and binary forms are not permitted in projects under
;;;    GNU General Public Licenses and its derivatives.
;;;
;;; THIS SOFTWARE IS PROVIDED BY ART OBREZAN ''AS IS'' AND ANY
;;; EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
;;; WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
;;; DISCLAIMED. IN NO EVENT SHALL ART OBREZAN BE LIABLE FOR ANY
;;; DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
;;; (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
;;; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
;;; ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
;;; (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
;;; SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


(in-package #:artisan-mysql)

;;-----------------------------------------------------------------------------
;; READ/WRITE/PARSE PACKETS
;;-----------------------------------------------------------------------------

(defun read-packet (stream)
  (multiple-value-bind (packet-length packet-number)
      (read-packet-header stream) ; TODO: return packet-number as a value?
    (declare (ignore packet-number))
    (let ((buf (make-array packet-length
                           :element-type '(unsigned-byte 8))))
      (read-sequence buf stream)
      buf)))

(defun read-packet-header (stream)
  (let ((len 0)
        (num 0))
    (setq len (+ (read-byte stream)
                 (ash (read-byte stream) 8)
                 (ash (read-byte stream) 16)))
    (setq num (read-byte stream))
    (values len num)))


(defun write-packet (data stream &key packet-number)
  (write-packet-header (length data) packet-number stream)
  (write-sequence data stream)
  (force-output stream))

(defun write-packet-header (len packet-number stream)
  (write-byte (logand #xff len) stream)
  (write-byte (logand #xff (ash len -8)) stream)
  (write-byte (logand #xff (ash len -16)) stream)
  (write-byte (logand #xff packet-number) stream))


(defun error-packet-p (buf)
  (= #xFF (aref buf 0)))

(defun parse-error-packet (buf)
  (let ((error (+ (aref buf 1)
                  (ash (aref buf 2) 8)))
        (sqlstate nil)
        (message nil))
    (if (char/= #\# (code-char (aref buf 3)))
        (setq message (decode-string buf :start 3 :format :utf8))
      (progn
        (setq sqlstate (decode-string buf :start 4 :end 8 :format :latin1))
        (setq message (decode-string buf :start 9 :format :utf8))))
    (list :error error
          :sqlstat sqlstate
          :message message)))


(defun ok-packet-p (buf)
  (zerop (aref buf 0)))

(defun parse-ok-packet (buf)
  (multiple-value-bind (affected-rows len)
      (decode-length-coded-binary buf 1)
    (multiple-value-bind (last-insert-id len2)
        (decode-length-coded-binary buf (+ 1 len))
      (let ((pos (+ 1 len len2)))
        (let ((server-status (+ (aref buf pos)
                                (ash (aref buf (+ pos 1)) 8)))
              (warning-count (+ (aref buf (+ pos 2))
                                (ash (aref buf (+ pos 3)) 8)))
              (message (when (< (+ pos 4) (length buf))
                         (decode-string buf
                                        :start (+ pos 4) :end (length buf)
                                        :format :utf8))))
          (list :affected-rows affected-rows
                :last-insert-id last-insert-id
                :server-status server-status
                :warning-count warning-count
                :message message))))))


(defun eof-packet-p (buf)
  (and (= #xFE (aref buf 0))
       (= 5 (length buf))))

(defun parse-eof-packet (buf)
  (let ((warning-count (+ (aref buf 1)
                          (ash (aref buf 2) 8)))
        (status (+ (aref buf 3)
                   (ash (aref buf 4) 8))))
    (list :warning-count warning-count
          :status status)))

