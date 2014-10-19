(defsystem #:artisan-mysql
    :serial t
    :name "MySQL Native Driver"
    :author "Art Obrezan"
    :version "1.0"
    :licence "MIT-style"
    :maintainer '("Dimitri Fontaine")
    :description      "MySQL Native Driver"
    :long-description "MySQL Native Driver for Common Lisp"
    :depends-on (#:babel
                 #:usocket
                 #:uiop)
    :components ((:file "package")
                 (:file "sha1")
                 (:file "string-utils")
                 (:file "bytes")
                 (:file "packets")
                 (:file "protocol")
                 (:file "query")
                 (:file "mysql")))
