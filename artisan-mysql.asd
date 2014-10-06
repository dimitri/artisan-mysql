(defsystem #:artisan-mysql
    :serial t
    :name "MySQL Native Driver"
    :author "Dimitri Fontaine"
    :version "1.0"
    :licence "MIT-style"
    :maintainer '("Dimitri Fontaine")
    :description      "MySQL Native Driver"
    :long-description "MySQL Native Driver for Common Lisp"
    :depends-on (#:babel
                 #:usocket
                 #:uiop)
    :components ((:file "package")
                 (:file "mysql" :depends-on ("package"))))
