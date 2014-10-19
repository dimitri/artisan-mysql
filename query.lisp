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

(defun string-append (&rest args)
  (apply #'concatenate 'string args))

(defun append-query-arguments (args)
  (apply #'string-append
         (mapcar #'(lambda (arg)
                     (string-append " " (if (stringp arg)
                                            arg
                                          (write-to-string arg))))
                 args)))

(defun doquery (connection query-string named-fileds-p)
  (let ((stream (mysqlcon-stream connection)))
    (unless stream
      (raise-mysql-error "No database connection"))
    (send-query-string query-string stream)
    (let ((packet (read-packet stream)))
      (cond ((error-packet-p packet)
             (raise-mysql-error packet))
            ((ok-packet-p packet)
             (update-connection-data connection packet))
            (t
             (parse-data-packets packet stream named-fileds-p))))))

(defun send-query-string (str stream)
  (write-packet
   (concatenate 'vector `#(,+com-query+) (encode-string str :format :utf8))
   stream
   :packet-number 0))

(defun update-connection-data (connection packet)
  (let* ((ok (parse-ok-packet packet))
         (last-insert-id (getf ok :last-insert-id)))
    (setf (mysqlcon-last-insert-id connection) last-insert-id))
  nil)

(defun parse-data-packets (packet stream named-fileds-p)
  (let ((num (decode-length-coded-binary packet 0)) ; number of columns
        (column-names-list nil))
    ;; read mysql field packets
    (dotimes (i num)
      (let ((buf (read-packet stream)))
        (when named-fileds-p
          (multiple-value-bind (start len) (nth-field-packet-entry 5 buf)
            (push (intern-to-keyword
                   (decode-string buf :start start :end (+ start len) :format :utf8))
                  column-names-list)))))
    (setq column-names-list (nreverse column-names-list))
    ;; read eof packet
    (read-packet stream)
    ;; read row packets data
    (loop :for packet := (read-packet stream)
       :until (eof-packet-p packet)
       :collect (parse-row-packet packet column-names-list))))

(defun intern-to-keyword (str)
  (intern (string-upcase str) "KEYWORD"))

(defun nth-field-packet-entry (n buf)
  (let ((pos 0) (len 0) (offset 0))
    (dotimes (i n)
      (incf pos (+ offset len))
      (multiple-value-bind (l o)
          (decode-length-coded-binary buf pos)
       (setq len l)
       (setq offset o)))
    (values (+ pos offset) len)))

(defun parse-row-packet (buf column-names-list)
  (let ((buf-len (length buf))
        (pos 0)
        (list nil))
    (loop
     (multiple-value-bind (len start) (decode-length-coded-binary buf pos)
       (if (= len -1) ;column value = NULL
           (progn
             (when column-names-list
               (push (pop column-names-list) list))
             (push "NULL" list)
             (setq pos (+ start pos)))
         (progn
           (when column-names-list
             (push (pop column-names-list) list))
           (push (decode-string buf :start (+ start pos) :end (+ start pos len) :format :utf8)
                 list)
           (setq pos (+ start pos len))))
       (when (>= pos buf-len) (return))))
     (nreverse list)))
