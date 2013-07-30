PROGNAME=$0
BINDIR=`python -c "import os;print os.path.abspath('.')"`

echo "[$PROGNAME]installing index.html..."
rm -rf ~/index.html
ln -s $BINDIR/index.html ~/index.html

echo "[$PROGNAME]installing hosts..."
sudo rm -rf /etc/hosts
kernel=`uname -s`
if [ "$kernel" = "Linux" ]
then
    sudo ln -s $BINDIR/hosts /etc/hosts
else
    # Darwin.
    sudo cp $BINDIR/hosts /etc/hosts
fi

echo "[$PROGNAME]installing .shrc"
rm -rf $HOME/.shrc
ln -s $BINDIR/.shrc $HOME/.shrc

echo "[$PROGNAME]installing .oh-my-zsh"
rm -rf $HOME/.oh-my-zsh
ln -s $BINDIR/.oh-my-zsh $HOME/.oh-my-zsh

echo "[$PROGNAME]installing .zshrc"
rm -rf $HOME/.zshrc
ln -s $BINDIR/.zshrc $HOME/.zshrc

echo "[$PROGNAME]installing settings.xml..."
mkdir -p $HOME/.m2
rm -rf $HOME/.m2/settings.xml
ln -s $BINDIR/settings.xml $HOME/.m2/settings.xml

echo "[$PROGNAME]installing config..."
mkdir -p $HOME/.ssh
rm -rf $HOME/.ssh/config
ln -s $BINDIR/config $HOME/.ssh/config


TARGETS="2utf8 cxxindent \
oprof mysqldb syslog \
local-install org2twiki \
 gc pom-create vpn-umeng-dp \
vpn-dirlt-aws proc-netstat rhs go-doc-server pcrypt einstall uinstall clj lein
patch-code"

INSTALLDIR=$HOME/utils/bin
if [ ! -d $INSTALLDIR ]
then
    mkdir -p $INSTALLDIR
fi
for target in $TARGETS
do
  echo "[$PROGNAME]installing $target..."
  rm -rf $INSTALLDIR/$target
  ln -s $BINDIR/$target $INSTALLDIR/$target
done

TARGETS=".emacs .emacs.d"
for target in $TARGETS
do
  echo "[$PROGNAME]installing $target..."
  rm -rf $HOME/$target
  ln -s $BINDIR/$target $HOME/$target
done

####################

echo "[$PROGNAME]installing dirlt-aws.pem..."
rm -rf $HOME/.ssh/dirlt-aws.pem
ln -s $HOME/.private/dirlt-aws.pem $HOME/.ssh/dirlt-aws.pem
chmod 0600 $HOME/.ssh/dirlt-aws.pem

echo "[$PROGNAME]installing id_rsa.pub..."
rm -rf $HOME/.ssh/id_rsa.pub
ln -s $HOME/.private/id_rsa.pub $HOME/.ssh/id_rsa.pub
chmod 0600 $HOME/.ssh/id_rsa.pub

echo "[$PROGNAME]installing id_rsa..."
rm -rf $HOME/.ssh/id_rsa
ln -s $HOME/.private/id_rsa $HOME/.ssh/id_rsa
chmod 0600 $HOME/.ssh/id_rsa

echo "[$PROGNAME]installing github.pub..."
rm -rf $HOME/.ssh/github.pub
ln -s $HOME/.private/github.pub $HOME/.ssh/github.pub
chmod 0600 $HOME/.ssh/github.pub

echo "[$PROGNAME]installing github..."
rm -rf $HOME/.ssh/github
ln -s $HOME/.private/github $HOME/.ssh/github
chmod 0600 $HOME/.ssh/github
