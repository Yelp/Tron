if [[ -n ${ZSH_VERSION-} ]]; then
	autoload -U +X bashcompinit && bashcompinit
fi

# This magic eval enables tab-completion for tron commands
# http://argcomplete.readthedocs.io/en/latest/index.html#synopsis
eval "$(/opt/venvs/tron/bin/register-python-argcomplete tronview)"
