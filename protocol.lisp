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
(defstruct mysqlcon
  socket
  stream
  host
  port
  connection-id
  server-capabilities
  last-insert-id)


;; See http://dev.mysql.com/doc/internals/en/connection-phase.html#capability-flags

(defconstant +capabilities+
  `((:client-long-password . #x1)
    (:client-found-rows . #x2)
    (:client-long-flag . #x4)
    (:client-connect-with-db . #x8)
    (:client-no-schema . #x10)
    (:client-compress . #x20)
    (:client-odbc . #x40)
    (:client-local-files . #x80)
    (:client-ignore-space . #x100)
    (:client-protocol-41 . #x200)
    (:client-interactive . #x400)
    (:client-ssl . #x800)
    (:client-ignore-sigpipe . #x1000)
    (:client-transactions . #x2000)
    (:client-reserved . #x4000)
    (:client-secure-connection . #x8000)
    (:client-multi-statements . #x10000)
    (:client-multi-results . #x20000)
    (:client-ps-multi-results . #x40000)
    (:client-plugin-auth . #x80000)
    (:client-connect-attrs . #x100000)
    (:client-plugin-auth-lenenc-client-data . #x200000)))

(defconstant +client-capabilities+
  '(:client-protocol-41
    :client-secure-connection ; Authentication::Native41
    :client-ignore-space
    :client-transactions))

(defun open-stream (host port)
  (let* ((socket (usocket:socket-connect host port
                                         :protocol :stream
                                         :timeout 3
                                         :element-type '(unsigned-byte 8)))
         (stream (usocket:socket-stream socket)))
    (unless stream
      (raise-mysql-error (format nil "Cannot connect to ~a:~a" host port)))
    (values socket stream)))

(defun initialize-connection (socket stream host port server-info)
  (make-mysqlcon :socket socket
                 :stream stream
                 :host host
                 :port port
                 :connection-id (getf server-info :thread-id)
                 :server-capabilities (getf server-info :server-capabilities)
                 :last-insert-id nil))


(defun parse-handshake-packet (buf)
  (let* ((protocol-version (aref buf 0))
         (pos (position 0 buf)) ;;; end position of a c-line (zero)
         (server-version (decode-string buf :start 1 :end pos :format :latin1))
         (thread-id (+ (aref buf (+ pos 1))
                       (ash (aref buf (+ pos 2)) 8)
                       (ash (aref buf (+ pos 3)) 16)
                       (ash (aref buf (+ pos 4)) 24)))
         (server-capabilities (number-to-capabilities
                               (+ (aref buf (+ pos 14))
                                  (ash (aref buf (+ pos 15)) 8))))
         (server-language (aref buf (+ pos 16)))
         (server-status (+ (aref buf (+ pos 17))
                           (ash (aref buf (+ pos 18)) 8)))
         (scramble (make-array 20 :element-type '(unsigned-byte 8))))
    (dotimes (i 8)
      (setf (aref scramble i) (aref buf (+ pos i 5))))
    (dotimes (i 12)
      (setf (aref scramble (+ i 8)) (aref buf (+ pos i 32))))
    (list :protocol-version protocol-version
          :server-version server-version
          :thread-id thread-id
          :server-capabilities server-capabilities
          :server-language server-language
          :server-status server-status
          :scramble scramble)))


(defun send-authorization-packet (stream user password database scramble)
  (write-packet
   (prepare-auth-packet user
                        (if (and (stringp password) (zerop (length password)))
                            nil password)
                        (if (and (stringp database) (zerop (length database)))
                            nil database)
                        scramble)
   stream
   :packet-number 1))

(defun prepare-auth-packet (user password database scramble)
  (let ((buf (make-array 32 :element-type '(unsigned-byte 8) :initial-element 0))
        (client-flags (capabilities-to-number
                       (if database
                           (cons :client-connect-with-db +client-capabilities+)
                         +client-capabilities+)))
        (database-buf (if database
                          (encode-string database :format :cstring)
                        #()))
        (user-buf (encode-string user :format :cstring))
        (auth-buf (if password
                      (let ((scramble-buf (password-to-token password scramble)))
                        (concatenate 'vector
                                     (vector (length scramble-buf))
                                     scramble-buf))
                    #(0))))
    (put-int32-to-array client-flags      buf :position 0) ;; capability flags
    (put-int32-to-array +max-packet-size+ buf :position 4) ;; max-packet size
    (put-int8-to-array  +utf8-general-ci+ buf :position 8) ;; character set
    (concatenate 'vector
                 buf
                 user-buf
                 auth-buf
                 database-buf)))

(defun password-to-token (password scramble)
  (let* ((pwd (encode-string password :format :latin1))
         (stage1-hash (sha1-digest pwd))
         (stage2-hash (sha1-digest stage1-hash))
         (digest (sha1-digest (concatenate 'vector scramble stage2-hash)))
         (token (make-array 20 :element-type '(unsigned-byte 8))))
    (dotimes (i (length token))
      (setf (aref token i)
            (logxor (aref digest i)
                    (aref stage1-hash i))))
    token))


(defun number-to-capabilities (num)
  (remove nil (mapcar #'(lambda (cons)
                          (if (zerop (logand (cdr cons) num))
                              nil
                            (car cons)))
                      +capabilities+)))

(defun capabilities-to-number (capabilities-list)
  (let ((num 0))
    (dolist (option capabilities-list)
      (incf num (cdr (assoc option +capabilities+))))
    num))


