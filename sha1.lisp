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
;;; SHA-1
;;; http://csrc.nist.gov/publications/fips/fips180-2/fips180-2.pdf
;;; implemented for byte messages
;;-----------------------------------------------------------------------------

(declaim (inline to-32bit-word))
(defun to-32bit-word (int)
  (logand #xFFFFFFFF int))

(declaim (inline sha1-rotl))
(defun sha1-rotl (n shift)
  (logior (to-32bit-word (ash n shift))
	  (ash n (- shift 32))))

(defun sha1-padding-size (n)
  (let ((x (mod (- 56 (rem n 64)) 64)))
    (if (zerop x) 64 x)))

(defun sha1-pad-message (message)
  (let* ((message-len (length message))
         (message-len-in-bits (* message-len 8))
         (buffer-len (+ message-len 8 (sha1-padding-size message-len)))
         (buffer (make-array buffer-len :initial-element 0)))
    (dotimes (i message-len)
      (setf (aref buffer i) (aref message i)))
    (setf (aref buffer message-len) #b10000000)
    (dotimes (i 8)
      (setf (aref buffer (- buffer-len (1+ i)))
            (logand #xFF (ash message-len-in-bits (* i -8)))))
    buffer))

(defun sha1-prepare-message-block (n data)
  (let ((message-block (make-array 80))
        (offset (* n 64)))
    (do ((i 0 (1+ i)))
        ((> i 15))
      (setf (aref message-block i)
            (+ (ash (aref data (+ offset   (* i 4))) 24)
               (ash (aref data (+ offset 1 (* i 4))) 16)
               (ash (aref data (+ offset 2 (* i 4))) 8)
               (aref data (+ offset 3 (* i 4))))))
    (do ((i 16 (1+ i)))
        ((> i 79))
      (setf (aref message-block i)
            (to-32bit-word
             (sha1-rotl (logxor (aref message-block (- i 3))
                           (aref message-block (- i 8))
                           (aref message-block (- i 14))
                           (aref message-block (- i 16))) 1))))
    message-block))

(defun sha1-f (n x y z)
  (cond ((<= 0 n 19)
         (to-32bit-word (logior (logand x y)
                                (logand (lognot x) z))))
        ((or (<= 20 n 39) (<= 60 n 79))
         (to-32bit-word (logxor x y z)))
        ((<= 40 n 59)
         (to-32bit-word (logior (logand x y)
                                (logand x z)
                                (logand y z))))))

(defun sha1-k (n)
  (cond ((<=  0 n 19) #x5A827999)
        ((<= 20 n 39) #x6ED9EBA1)
        ((<= 40 n 59) #x8F1BBCDC)
        ((<= 60 n 79) #xCA62C1D6)))

(defun sha1-digest (message)
  "Make a SHA1 digest from a vector of bytes"
  (let* ((h0 #x67452301)
         (h1 #xEFCDAB89)
         (h2 #x98BADCFE)
         (h3 #x10325476)
         (h4 #xC3D2E1F0)
         (padded-message (sha1-pad-message message))
         (n (/ (length padded-message) 64)))
    (dotimes (i n)
      (let ((a h0) (b h1) (c h2) (d h3) (e h4) (temp 0)
            (message-block (sha1-prepare-message-block i padded-message)))
        (dotimes (i 80)
          (setq temp (to-32bit-word (+ (sha1-rotl a 5)
                                       (sha1-f i b c d)
                                       e
                                       (sha1-k i)
                                       (aref message-block i))))
          (setq e d)
          (setq d c)
          (setq c (to-32bit-word (sha1-rotl b 30)))
          (setq b a)
          (setq a temp))
        (setq h0 (to-32bit-word (+ h0 a)))
        (setq h1 (to-32bit-word (+ h1 b)))
        (setq h2 (to-32bit-word (+ h2 c)))
        (setq h3 (to-32bit-word (+ h3 d)))
        (setq h4 (to-32bit-word (+ h4 e)))))
    (vector
     (logand #xFF (ash h0 -24))
     (logand #xFF (ash h0 -16))
     (logand #xFF (ash h0 -8))
     (logand #xFF h0)
     (logand #xFF (ash h1 -24))
     (logand #xFF (ash h1 -16))
     (logand #xFF (ash h1 -8))
     (logand #xFF h1)
     (logand #xFF (ash h2 -24))
     (logand #xFF (ash h2 -16))
     (logand #xFF (ash h2 -8))
     (logand #xFF h2)
     (logand #xFF (ash h3 -24))
     (logand #xFF (ash h3 -16))
     (logand #xFF (ash h3 -8))
     (logand #xFF h3)
     (logand #xFF (ash h4 -24))
     (logand #xFF (ash h4 -16))
     (logand #xFF (ash h4 -8))
     (logand #xFF h4))))
