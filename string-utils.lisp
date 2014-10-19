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
;; STRING UTILS
;;-----------------------------------------------------------------------------

(defun encode-string (str &key format)
  (ecase format
    (:latin1 (strutils-encode-latin1 str))
    (:cstring (strutils-encode-cstring str))
    (:utf8 (strutils-encode-utf8 str))))

(defun decode-string (buf &key format (start 0) (end (length buf)))
  (ecase format
    (:latin1 (strutils-decode-latin1 buf start end))
    (:utf8 (strutils-decode-utf8 buf start end))))


(defun strutils-encode-latin1 (str)
  (let ((buf (make-array (length str) :element-type '(unsigned-byte 8))))
    (dotimes (i (length str))
      (let ((code (char-code (char str i))))
        (if (< code 256)
            (setf (aref buf i) code)
          (error "~S is not a valid latin1 character" (char str i)))))
    buf))

(defun strutils-decode-latin1 (buf start end)
  (let ((str (make-string (- end start))))
    (dotimes (i (- end start))
      (setf (char str i) (code-char (aref buf (+ i start)))))
    str))

(defun strutils-encode-cstring (str)
  (let ((buf (make-array (1+ (length str))
                         :element-type '(unsigned-byte 8)
                         :initial-element 0)))
    (dotimes (i (length str))
      (let ((code (char-code (char str i))))
        (if (< code 256)
            (setf (aref buf i) code)
          (error "~S is not a valid latin1 character" (char str i)))))
    buf))

(defun strutils-encode-utf8 (str)
  (declare (optimize (speed 3) (debug 0) (safety 0) (float 0)))
  (let ((buf (make-array (* 3 (length str)) :element-type '(unsigned-byte 8)))
        (pos 0))
    (declare (type fixnum pos))
    (declare (type (vector (unsigned-byte 8) *) buf))
    (dotimes (i (length str))
      (let ((code (char-code (schar str i))))
        (cond
         ((< code #x80)
          (setf (aref buf pos) code)
          (incf pos))
         ((< code #x800)
          (setf (aref buf pos) (logior 192 (ash (logand 1984 code) -6)))
          (incf pos)
          (setf (aref buf pos) (logior 128 (logand 63 code)))
          (incf pos))
         ((< code #x10000)
          (setf (aref buf pos) (logior 224 (ash (logand 61440 code) -12)))
          (incf pos)
          (setf (aref buf pos) (logior 128 (ash (logand 4032 code) -6)))
          (incf pos)
          (setf (aref buf pos) (logior 128 (logand 63 code)))
          (incf pos))
         (t (error "Character is out of the ucs-2 range")))))
    (subseq buf 0 pos)))

(defconstant +utf8-sequence-length+
  (vector
    1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1
    1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1
    1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1
    1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1
    1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1
    1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1
    1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1
    1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1
    1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1
    1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1
    1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1
    1 1 1 1 1 1 1 1 1 1 1 1 1 1 1 1
    2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2
    2 2 2 2 2 2 2 2 2 2 2 2 2 2 2 2
    3 3 3 3 3 3 3 3 3 3 3 3 3 3 3 3
    4 4 4 4 4 4 4 4 5 5 5 5 6 6 1 1))

(defun strutils-decode-utf8 (buf start end)
  (declare (optimize (speed 3) (debug 0) (safety 0) (float 0)))
  (declare (type fixnum start end))
  (declare (type (vector (unsigned-byte 8) *) buf))
  (let ((pos start)
        (i 0)
        (str (make-array (the fixnum (- end start)) :element-type 'base-char)))
    (declare (type fixnum pos i))
    (loop (when (>= pos end)
            (return (subseq str 0 i)))
          (let* ((byte1 (aref buf pos))
                 (sequence-length (svref +utf8-sequence-length+ byte1)))
            (declare (type fixnum byte1 sequence-length))
            (cond
             ((= 1 sequence-length)
              (setf (schar str i) (code-char byte1)))
             ((= 2 sequence-length)
              (let* ((byte2 (aref buf (the fixnum (1+ pos))))
                     (code  (logior (the fixnum (ash (the fixnum (logand #b00011111 byte1)) 6))
                                    (the fixnum (logand #b00111111 byte2)))))
                (declare (type fixnum byte2 code))
                (setf (schar str i) (code-char code))))
             ((= 3 sequence-length)
              (let* ((byte2 (aref buf (the fixnum (1+ pos))))
                     (byte3 (aref buf (the fixnum (1+ (the fixnum (1+ pos))))))
                     (code  (logior (the fixnum (ash (logand #b00001111 byte1) 12))
                                    (the fixnum (logior (the fixnum (ash (the fixnum (logand #b00111111 byte2)) 6))
                                                        (the fixnum (logand #b00111111 byte3)))))))
                (declare (type fixnum byte2 byte3 code))
                (setf (schar str i) (code-char code))))
             (t
              (setf (schar str i) #\Space)))
            (incf i)
            (incf pos sequence-length)))))
