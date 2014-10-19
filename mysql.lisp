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
;; CONNECT
;;-----------------------------------------------------------------------------

(defconstant +max-packet-size+ (* 1024 1024)) ;; in bytes
(defconstant +utf8-general-ci+ 33)


(defun connect (&key host (port 3306) user (password nil) (database nil))
  (multiple-value-bind (socket stream) (open-stream host port)
    (let* ((packet (read-packet stream)))
      (if (error-packet-p packet)
          (raise-mysql-error packet)
          (let* ((server-info (parse-handshake-packet packet))
                 (scramble (getf server-info :scramble)))
            (send-authorization-packet stream user password database scramble)
            (let ((packet (read-packet stream))
                  (connection nil))
              (cond ((ok-packet-p packet)
                     (setf connection
                           (initialize-connection socket stream host port
                                                  server-info)))
                    ((error-packet-p packet)
                     (usocket:socket-close socket)
                     (raise-mysql-error packet))
                    (t
                     (usocket:socket-close socket)
                     (raise-mysql-error "Unknown error during connection.")))
              (query connection "SET NAMES 'utf8'")
              connection))))))

;;-----------------------------------------------------------------------------
;; DISCONNECT
;;-----------------------------------------------------------------------------

(defconstant +com-quit+ 1)

(defun disconnect (connection)
  (when (mysqlcon-stream connection)
    (send-quit connection)
    (usocket:socket-close (mysqlcon-socket connection))
    (setf (mysqlcon-socket connection) nil
          (mysqlcon-stream connection) nil)))

(defun send-quit (connection)
  (write-packet `#(,+com-quit+) (mysqlcon-stream connection)
                :packet-number 0))


;;-----------------------------------------------------------------------------
;; QUERY
;;-----------------------------------------------------------------------------

(defconstant +com-query+ 3)

(defun query (connection &rest args)
  (let ((query-string (append-query-arguments args)))
    (doquery connection query-string nil)))

(defun pquery (connection &rest args)
  (let ((query-string (append-query-arguments args)))
    (doquery connection query-string t)))

;;-----------------------------------------------------------------------------
;; MYSQL ADDON FUNTIONS/MACROS
;;-----------------------------------------------------------------------------

(defun last-insert-id (connection)
  (mysqlcon-last-insert-id connection))

;; Escape string to insert into a string column
(defun escape-string (str)
  (let ((escaped-string (make-array (length str)
                                    :element-type 'base-char
                                    :adjustable t
                                    :fill-pointer 0)))
    (dotimes (i (length str))
      (let ((ch (char str i)))
        (when (member ch '(#\' #\" #\\))
          (vector-push-extend #\\ escaped-string))
        (vector-push-extend ch escaped-string)))
    escaped-string))

(defun quote-string (str)
  (string-append "'" (escape-string str) "'"))

(defmacro with-connection ((db credentials) &body body)
  `(let ((,db (apply #'artisan-mysql:connect ,credentials)))
     (unwind-protect (progn ,@body)
       (when ,db (artisan-mysql:disconnect ,db)))))

(defmacro with-transaction ((db) &body body)
  (with-unique-names (res)
    `(let ((,res nil))
       (unwind-protect
           (prog2
                (artisan-mysql:query ,db "START TRANSACTION")
                (progn ,@body)
             (setf ,res t))
         (artisan-mysql:query ,db (if ,res "COMMIT" "ROLLBACK"))))))



;;-----------------------------------------------------------------------------
;; ERRORS
;;-----------------------------------------------------------------------------

(define-condition mysql-error (error)
  ((number  :initarg :number  :initform nil :reader mysql-error-number)
   (message :initarg :message :initform nil :reader mysql-error-message))
  (:report (lambda (condition stream)
             (format stream "MySQL~:[~;~:*(~A)~]: ~a~%"
                     (mysql-error-number condition)
                     (mysql-error-message condition)))))

(defun raise-mysql-error (obj)
  (typecase obj
    (string
     (error 'mysql-error
            :message obj))
    (vector
     (let ((error-info (parse-error-packet obj)))
       (error 'mysql-error
              :number (getf error-info :error)
              :message (getf error-info :message))))
    (t
     (error 'mysql-error))))
