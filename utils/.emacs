;;; .emacs

(setq mac-system nil)
;; (if (string-equal (getenv "HOME") "/Users/dirlt")
;;     (setq mac-system t))
(when (eq system-type 'darwin)
  (setq mac-system t))

;;; common code.
;; sudo apt-get install emacs-goodies-el
(setq load-path (cons "~/.emacs.d/" load-path))
(setq load-path (cons "~/.emacs.d/emacs-goodies-el-35.0/elisp/emacs-goodies-el/" load-path))
(require 'xml-parse)
(autoload 'make-regexp "make-regexp"
  "Return a regexp to match a string item in STRINGS.")
(autoload 'make-regexps "make-regexp"
  "Return a regexp to REGEXPS.")
(require 'syntax)

;;; perference.
;; (setq inhibit-default-init t)
(when (fboundp 'global-font-lock-mode) 
  (global-font-lock-mode t))
(setq frame-title-format (concat  "emacs@%b" system-name))
;; (normal-erase-is-backspace-mode)
(setq transient-mark-mode t)
(setq column-number-mode t)
(setq default-fill-column 80)
(setq hl-line-mode t)
(setq bookmark-save-flag 1) 
(setq inhibit-startup-message t)
(setq inhibit-splash-screen t)
(setq x-select-enable-clipboard t) ;;允许复制到外部剪贴板
(setq default-major-mode 'text-mode)
;;(add-hook 'text-mode-hook 'turn-on-auto-fill) ;;自动换行.
(setq initial-major-mode 'emacs-lisp-mode)
(add-to-list 'auto-mode-alist '("\\.el\\'" . emacs-lisp-mode))
(setq visible-bell nil)
(setq kill-ring-max 200)
(show-paren-mode t)
(setq show-paren-style 'parentheses)

;; turn off tool-bar, menu-bar, and scroll-bar
(tool-bar-mode 0)
(menu-bar-mode 1)
(scroll-bar-mode -1)
(auto-image-file-mode t) ;; 自动打开图片
(setq version-control t) ;; backup使用版本管理
;; confused by following settings.!!!
(setq kept-new-versions 3)
(setq delete-old-versions t)
(setq kept-old-versions 2)
(setq dired-kept-versions 1)
;; (setq make-backup-files nil) 
(setq backup-directory-alist (quote (("." . "~/.backups"))))
(setq user-full-name "dirtysalt") 
(setq user-mail-address "dirtysalt1987@gmail.com")
(setq dired-recursive-copies 'top)
(setq dired-recursive-deletes 'top)

;;; htmlize.
(require 'htmlize)

;;; recentf.
(require 'recentf)
(recentf-mode 1)

;;; go.
(require 'go-mode-load)

;;; clojure.
(require 'clojure-mode)
(require 'clojure-test-mode)

;; ;;; anything.
;; ;; sudo apt-get install anything-el
;; (require 'anything)
;; (require 'anything-config)
;; (global-set-key "\C-cat" 'anything)

;;; google c style.
(require 'google-c-style)
(setq c-default-style "java")
(defun my-c-mode-common-hook()
  (setq tab-width 2)
  (setq indent-tabs-mode nil)
  (setq c-basic-offset 2))

(add-hook 'c-mode-common-hook 'my-c-mode-common-hook)
(add-hook 'c-mode-common-hook 'google-set-c-style)
(add-hook 'c-mode-common-hook 'google-make-newline-indent)
(setq auto-mode-alist  (append '(("\\.h\\'" . c++-mode)) 
                               '(("\\.hpp\\'" . c++-mode))
                               '(("\\.c\\'" . c++-mode)) 
                               '(("\\.cc\\'" . c++-mode)) 
                               '(("\\.cpp\\'" . c++-mode))
                               auto-mode-alist))
(setq-default nuke-trailing-whitespace-p t)
;; never bother repeat it.
(setq tab-width 2)
(setq indent-tabs-mode nil)
(setq c-basic-offset 2)

;;; doxymacs.
;; sudo apt-get install doxymacs
;; - Default key bindings are:
;;   - C-c d ? will look up documentation for the symbol under the point.
;;   - C-c d r will rescan your Doxygen tags file.
;;   - C-c d f will insert a Doxygen comment for the next function.
;;   - C-c d i will insert a Doxygen comment for the current file.
;;   - C-c d ; will insert a Doxygen comment for the current member.
;;   - C-c d m will insert a blank multiline Doxygen comment.
;;   - C-c d s will insert a blank singleline Doxygen comment.
;;   - C-c d @ will insert grouping comments around the current region.
;; (require 'doxymacs)
;; (add-hook 'c-mode-common-hook 'doxymacs-mode)
;; (add-hook 'python-mode-hook 'doxymacs-mode)
;; (add-hook 'java-mode-hook 'doxymacs-mode)
;; (defun my-doxymacs-font-lock-hook ()
;;   (if (or (eq major-mode 'c-mode) 
;;           (eq major-mode 'c++-mode)
;;           (eq major-mode 'python-mode)
;;           (eq major-mode 'java-mode))
;;       (doxymacs-font-lock)))
;; (add-hook 'font-lock-mode-hook 'my-doxymacs-font-lock-hook)

;;; hippie-expand.
(setq hippie-expand-try-functions-list
      '(try-expand-dabbrev-visible 
        try-expand-dabbrev 
        try-expand-dabbrev-all-buffers 
        try-expand-dabbrev-from-kill 
        try-complete-file-name-partially 
        try-complete-file-name 
        try-expand-all-abbrevs 
        try-expand-list 
        try-expand-line 
        try-complete-lisp-symbol-partially 
        try-complete-lisp-symbol
        try-expand-whole-kill))

;;; color-theme. http://alexpogosyan.com/color-theme-creator
(setq load-path (cons "~/.emacs.d/color-theme-6.6.0" load-path))
(require 'color-theme)
(color-theme-initialize)
(color-theme-billw)

;;; auto-complete.
;; sudo apt-get install auto-complete-el
;; http://cx4a.org/software/auto-complete/manual.html
(setq load-path (cons "~/.emacs.d/auto-complete-1.3.1" load-path))
(require 'auto-complete-config)
(ac-config-default)
(add-to-list 'ac-dictionary-directories "~/.emacs.d/ac-dict")
(global-auto-complete-mode 1)
(setq ac-modes
      (append ac-modes '(org-mode 
                         objc-mode 
                         sql-mode
                         c++-mode
                         java-mode
                         python-mode
                         change-log-mode 
                         text-mode
                         makefile-mode
                         autoconf-mode)))


;;; yacc-mode.
(require 'yacc-mode)
(add-to-list 'auto-mode-alist '("\\.y$" . yacc-mode))

;;; flex-mode.
(require 'flex-mode)
(add-to-list 'auto-mode-alist '("\\.l$" . flex-mode))

;;; cmake-mode.
(require 'cmake-mode)

;; ;;; python-mode.
;; ;; sudo apt-get install python-mode
;; (setq load-path (cons "~/.emacs.d/python-mode.el-6.0.11" load-path))
;; (require 'python-mode)

;;; php-mode
;; sudo apt-get install php-elisp
(require 'php-mode)

;; ;;; cedet.
;; (setq load-path (cons "~/.emacs.d/cedet-1.1/common" load-path))
;; (require 'cedet)

;;; ido.
(setq load-path (cons "~/.emacs.d/ido" load-path))
(require 'ido)
(ido-mode t)
;; (require 'ido-ubiquitous)
;; (ido-ubiquitous t)
;; (require 'init-ido)
;; (require 'smex)

;;; cscope.
;;; NOTE(dirlt):但是其实索引效果没有那么好，cscope对于C支持很好，对C++就已经有点吃力了。
;; sudo apt-get install cscope-el
(require 'xcscope)
;; C-c s a //设定初始化的目录，一般是你代码的根目录
;; C-s s I //对目录中的相关文件建立列表并进行索引
;; C-c s s //序找符号
;; C-c s g //寻找全局的定义
;; C-c s c //看看指定函数被哪些函数所调用
;; C-c s C //看看指定函数调用了哪些函数
;; C-c s e //寻找正则表达式
;; C-c s f //寻找文件
;; C-c s i //看看指定的文件被哪些文件include
;; C-c s u //回到上一个跳转点(pop up mark)
;; C-c s p //回到上一个symbol
;; C-c s P //回到上一个文件
;; C-c s n //下一个symbol
;; C-c s N //下一个文件
(setq cscope-do-not-update-database t) ;; don't need to update database
;; cscope仅仅对于C/C++文件有用,对于其他文件的话可以使用etags
;; etags FILES这样会索引FILES生成TAGS文件
;; M-x visit-tags-table //提示载入TAGS文件
;; M-. //查找相应函数定义
;; C-u M-. //如果错误的话,那么查找下一个
;; M-* //返回之前的位置

;;; ibuffer.
(require 'ibuffer)
(global-set-key (kbd "C-x C-b") 'ibuffer)

;;; ibus.
;; sudo apt-get install ibus-el
(if (not mac-system)
    (progn
      (require 'ibus)
      (add-hook 'after-init-hook 'ibus-mode-on)
      ;; Use C-SPC for Set Mark command
      ;; (ibus-define-common-key ?\C-\s nil)
      ;; Use C-/ for Undo command
      (ibus-define-common-key ?\C-/ nil) ;; 绑定在undo上面
      (global-set-key [(shift)] 'ibus-toggle)
      ;; Change cursor color depending on IBus status
      (setq ibus-cursor-color '("red" "blue" "limegreen"))))

;;; session.
(require 'session)
(add-hook 'after-init-hook 'session-initialize)

;;; nxml mode
;; (defvar nxml-mode-map
;;       (let ((map (make-sparse-keymap)))
;;         (define-key map "\M-\C-u"  'nxml-backward-up-element)
;;         (define-key map "\M-\C-d"  'nxml-down-element)
;;         (define-key map "\M-\C-n"  'nxml-forward-element)
;;         (define-key map "\M-\C-p"  'nxml-backward-element)
;;         (define-key map "\M-{"     'nxml-backward-paragraph)
;;         (define-key map "\M-}"     'nxml-forward-paragraph)
;;         (define-key map "\M-h"     'nxml-mark-paragraph)
;;         (define-key map "\C-c\C-f" 'nxml-finish-element)
;;         (define-key map "\C-c\C-b" 'nxml-balanced-close-start-tag-block)
;;         (define-key map "\C-c\C-i" 'nxml-balanced-close-start-tag-inline)
;;         (define-key map "\C-c\C-x" 'nxml-insert-xml-declaration)
;;         (define-key map "\C-c\C-d" 'nxml-dynamic-markup-word)
;;         (define-key map "\C-c\C-u" 'nxml-insert-named-char)
;;         (define-key map "/"        'nxml-electric-slash)
;;         (define-key map [C-return] 'nxml-complete) 
;;         (when nxml-bind-meta-tab-to-complete-flag
;;           (define-key map "\M-\t"  'nxml-complete))
;;         map)
;;       "Keymap used by NXML Mode.")
(setq load-path (cons "~/.emacs.d/nxml-mode-20041004" load-path))
(require 'nxml-mode)
(setq nxml-child-indent 2)
(setq auto-mode-alist
      (cons '("\\.\\(xml\\|xsl\\|rng\\|xhtml\\|html\\|htm\\)\\'" . nxml-mode)
            auto-mode-alist))


;;; multi-term.
(require 'multi-term)
(setq multi-term-program "/bin/zsh")
;; (setq multi-term-program "/bin/bash")
(setq multi-term-buffer-name "multi-term")
;; 打开之后直接定位到这个窗口
(setq multi-term-dedicated-select-after-open-p t) 

(defun multi-eshell ()
  (interactive)
  (progn
    (eshell)
    ;; rename eshell buffer.
    (rename-buffer (generate-new-buffer-name "eshell-"))))
(global-set-key "\C-x." 'multi-term)
;; (global-set-key "\C-x," 'multi-eshell)

;;; protobuf-mode.
(require 'protobuf-mode)
(setq auto-mode-alist 
      (cons '("\\.proto\\'" . protobuf-mode) 
            auto-mode-alist))

;;; markdown-mode.
(require 'markdown-mode)
(setq auto-mode-alist 
      (cons '("\\.md\\'" . markdown-mode) 
            auto-mode-alist))

;;; global keybindings.
(global-set-key "\M-g" 'goto-line)
(global-set-key "\M-m" 'compile)
(global-set-key "\M-/" 'hippie-expand)
(global-set-key "\C-xp" 'previous-error) 
(global-set-key "\C-xn" 'next-error)
;; (global-set-key "\C-cbml" 'list-bookmarks) ;; book mark list.
;; (global-set-key "\C-cbms" 'bookmark-set) ;; book mark set.
;; (global-set-key "\C-chdf" 'describe-function) ;; help describe function.
;; (global-set-key "\C-chdv" 'describe-variable) ;; help describe variable.
;; (global-set-key "\C-chdk" 'describe-key) ;; help describe key.
(global-set-key "\C-c;" 'comment-or-uncomment-region)
(defun run-current-file ()
  (interactive)
  (setq ext-map
	'(("clj" . " ~/utils/bin/clj")))
  (setq file-name (buffer-file-name))
  (setq file-ext (file-name-extension file-name))
  (setq prog-name (cdr (assoc file-ext ext-map)))
  (setq command (concat prog-name " " file-name))
  (shell-command command))
(global-set-key "\C-c\C-c" 'run-current-file)


;;; encoding.
(set-language-environment "UTF-8")
(setq current-language-environment "UTF-8")
(setq locale-coding-system 'utf-8)
(set-terminal-coding-system 'utf-8)
(set-keyboard-coding-system 'utf-8)
(set-selection-coding-system 'utf-8)
(prefer-coding-system 'utf-8)

;;; default browser.
(setq browse-url-browser-function 
      'browse-url-generic)
(setq browse-url-generic-program 
      (executable-find "google-chrome"))

(if mac-system
    (setq browse-url-browser-function 'browse-url-default-macosx-browser))

;;; org-mode. I have to include it because I've changed the code.
;; sudo apt-get install org-mode
;; BEGIN_VERSE 下面两种都是引用原文格式
;; BEGIN_QUOTE
;; BEGIN_CENTER 引用文字居中
;; C-C C-e t // 插入export模版
;; C-c C-n //下一个标题
;; C-c C-p //上一个标题
;; C-c C-f //同级下一个标题
;; C-c C-b //同级上一个标题
;; C-c C-u //高一层标题
;; C-c C-o //打开连接
;; C-c C-l //查看连接
;; C-cxa // 日程安排 org-agenda
;; C-c C-e // export.
;; C-c C-c // 在脚注的标记与内容之间进行切换
;; #+CAPTION: 表格标题，可以用在图片或者是表格上面
;; S-M-RET 在同级下面创建一个TODO项目
;; S-up/down 修改项目的优先级
;; *text* bold mode.
;; /text/ italic mode.
;; _text_ underline mode.
;; #<<anchor>>
;; #+STYLE: <link rel="stylesheet" type="text/css" href="./site.css" />
(setq load-path (cons "~/.emacs.d/org-7.9.2/" load-path))
(require 'org-install)
(require 'org-publish)
(define-key global-map "\C-ca" 'org-agenda)
;; (add-to-list 'auto-mode-alist '("\\.org\\'" . org-mode))
(add-to-list 'auto-mode-alist '("\\.org$" . org-mode))
(add-hook 'org-mode-hook 'turn-on-font-lock)
(add-hook 'org-mode-hook 
          (lambda () (setq truncate-lines nil)))
(setq org-log-done t)
;;(setq org-agenta-files (file-expand-wildcards "~/github/sperm/essay/*.org"))
;;(setq org-agenta-files "~/github/sperm/essay/plan.org")
(setq org-export-have-math nil)
(setq org-use-sub-superscripts (quote {}))
(setq org-export-author-info nil)
(setq org-publish-project-alist
      '(("essay"
         :base-directory "~/github/sperm/essay"
         :publishing-directory "~/github/sperm/www/"
         :section-numbers 't
	 :style "<link rel=\"stylesheet\" type=\"text/css\" href=\"./site.css\" />"
         :table-of-contents 't)
        ("note"
         :base-directory "~/github/sperm/essay/note"
         :publishing-directory "~/github/sperm/www/note"
         :section-numbers 't
	 :style "<link rel=\"stylesheet\" type=\"text/css\" href=\"../site.css\" />"
         :table-of-contents 't)))

;; auto indent
;;(setq org-startup-indented t)
(global-set-key "\C-coi" 'org-indent-mode) ;; 切换indent视图
(autoload 'iimage-mode "iimage" "Support Inline image minor mode." t)
(autoload 'turn-on-iimage-mode "iimage" "Turn on Inline image minor mode." t)
(global-set-key "\C-cii" 'iimage-mode) ;; 允许在org-mode里面显示图片

;; ;; arrange for the clock information to persist across Emacs sessions
;; (setq org-clock-persist t)
;; (org-clock-persistence-insinuate)

;;; yasnippet
;; sudo apt-get install yasnippet
;; http://capitaomorte.github.com/yasnippet/
(setq load-path (cons "~/.emacs.d/yasnippet-0.6.1c/" load-path))
(require 'yasnippet)
(yas/initialize)
(setq yas/root-directory (cons "~/.emacs.d/snippets"
                               yas/root-directory))
(yas/load-directory "~/.emacs.d/snippets")
;; default TAB key is occupied by auto-complete
;; yas/insert-snippet ; insert a template
;; yas/new-snippet ; create a template
(global-set-key "\C-c," 'yas/expand)
(global-set-key "\C-cye" 'yas/expand)

;; thanks to capitaomorte for providing the trick.
(defadvice yas/insert-snippet (around use-completing-prompt activate)
  "Use `yas/completing-prompt' for `yas/prompt-functions' but only here..."
  (let ((yas/prompt-functions '(yas/completing-prompt)))
    ad-do-it))

;; give yas/dropdown-prompt in yas/prompt-functions a chance
(require 'dropdown-list)
;; use yas/completing-prompt when ONLY when `M-x yas/insert-snippet'
;; thanks to capitaomorte for providing the trick.
(defadvice yas/insert-snippet (around use-completing-prompt activate)
  "Use `yas/completing-prompt' for `yas/prompt-functions' but only here..."
  (let ((yas/prompt-functions '(yas/completing-prompt)))
    ad-do-it))

;;; desktop.
(require 'desktop)
(desktop-save-mode t)

(if mac-system
    (global-set-key [(f10)] 'ns-toggle-fullscreen))

;; key bindings
(when (eq system-type 'darwin) ;; mac specific settings
  ;; (setq mac-option-modifier 'alt)
  (setq mac-command-modifier 'meta)
  (global-set-key [kp-delete] 'delete-char) ;; sets fn-delete to be right-delete
  )
;;; systemtap.
(require 'systemtap-mode)
(add-to-list 'auto-mode-alist '("\\.stp$" . systemtap-mode))

;;; rainbow-delimiters
;; (require 'rainbow-delimiters)
;; (global-rainbow-delimiters-mode)

;;; ac-slime
;; (require 'ac-slime)

;;; starter-kit
;; (setq load-path (cons "~/.emacs.d/starter-kit" load-path))
;; (require 'starter-kit)
;; (require 'starter-kit-lisp)
;; (require 'starter-kit-eshell)
;; (require 'starter-kit-bindings)

;;; paredit-mode
;; (require 'paredit)
;; (disable-paredit-mode)
;; (add-hook 'clojure-mode-hook 'enable-paredit-mode)
;; (add-hook 'clojure-test-mode-hook 'enable-paredit-mode)

(require 'package)
(add-to-list 'package-archives
             '("marmalade" . "http://marmalade-repo.org/packages/") t)
;; (package-refresh-contents)
(package-initialize)
(custom-set-variables
 ;; custom-set-variables was added by Custom.
 ;; If you edit it by hand, you could mess it up, so be careful.
 ;; Your init file should contain only one such instance.
 ;; If there is more than one, they won't work right.
 '(session-use-package t nil (session)))
(custom-set-faces
 ;; custom-set-faces was added by Custom.
 ;; If you edit it by hand, you could mess it up, so be careful.
 ;; Your init file should contain only one such instance.
 ;; If there is more than one, they won't work right.
 )
