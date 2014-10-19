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
;; BYTE UTILS
;;-----------------------------------------------------------------------------

(defun put-int8-to-array (int array &key position)
  (setf (aref array position) (logand #xFF int)))

(defun put-int32-to-array (int array &key position)
  (setf (aref array position) (logand #xFF int))
  (setf (aref array (+ position 1)) (logand #xFF (ash int -8)))
  (setf (aref array (+ position 2)) (logand #xFF (ash int -16)))
  (setf (aref array (+ position 3)) (logand #xFF (ash int -24))))

(defun decode-length-coded-binary (buf pos)
  (declare (optimize (speed 3) (debug 0) (float 0)))
  (let ((val (aref buf pos)))
    (cond ((< val 251) (values val 1))
          ((= val 251) (values -1 1)) ;column value = NULL (only appropriate in a Row Data Packet)
          ((= val 252) (values (get-length-coded-int16 buf pos) 3))
          ((= val 253) (values (get-length-coded-int24 buf pos) 4))
          ((= val 254) (values (get-length-coded-int64 buf pos) 9)))))

(defun get-length-coded-int16 (buf pos)
  (+ (aref buf (+ 1 pos))
     (ash (aref buf (+ 2 pos)) 8)))

(defun get-length-coded-int24 (buf pos)
  (+ (aref buf (+ 1 pos))
     (ash (aref buf (+ 2 pos)) 8)
     (ash (aref buf (+ 3 pos)) 16)))

(defun get-length-coded-int64 (buf pos)
  (+ (aref buf (+ 1 pos))
     (ash (aref buf (+ 2 pos)) 8)
     (ash (aref buf (+ 3 pos)) 16)
     (ash (aref buf (+ 4 pos)) 24)
     (ash (aref buf (+ 5 pos)) 32)
     (ash (aref buf (+ 6 pos)) 40)
     (ash (aref buf (+ 7 pos)) 48)
     (ash (aref buf (+ 8 pos)) 56)))
