"""Offline pgBackRest round-trip in a new PRIVATE rehearsal repository only."""
import asyncio
import json
from pathlib import Path
import re


def private_paths(source, work, restored):
    source,work,restored=map(Path,(source,work,restored))
    for path in (source,restored):
        if (path.name!='pgdata' or path.parent.parent!=Path('/restore-test')
                or not path.parent.name.startswith('db-refactor-baseline-')
                or path.resolve()!=path or path.is_symlink()):
            raise ValueError('Backup/restore requires distinct private baseline workspaces')
    if (source==restored or work.parent!=Path('/restore-test')
            or not re.fullmatch(r'db-refactor-acceptance-[a-f0-9]{12}',work.name)
            or work.resolve()!=work or work.is_symlink()):
        raise ValueError('Invalid isolated repository workspace')
    return source,work,restored


class OfflineRoundTrip:
    def __init__(self, source, socket, work, restored, publish, status):
        self.source,self.work,self.restored=private_paths(source,work,restored)
        self.socket=Path(socket)
        if self.socket!=self.source.parent/'socket':
            raise ValueError('Unexpected source socket directory')
        self.publish,self.status=publish,status
        self.config=self.work/'pgbackrest.conf'

    def prepare(self):
        if self.work.exists() or self.restored.parent.exists():
            raise RuntimeError('Backup and restore workspaces must be new; nothing is overwritten')
        self.work.mkdir(mode=0o700)
        self.restored.parent.mkdir(mode=0o700)
        self.restored.mkdir(mode=0o700)
        (self.restored.parent/'socket').mkdir(mode=0o700)
        # Explicit config means the image's production repo/stanza defaults are
        # never consulted. Only this dedicated private path is writable.
        self.config.write_text(f'''[global]
repo1-path={self.work}/repository
repo1-retention-full=2
compress-type=zst
compress-level=1
process-max=2
log-level-console=detail
log-level-file=off
lock-path={self.work}/lock
log-path={self.work}/log

[refactor]
pg1-path={self.source}
pg1-port=55432
pg1-socket-path={self.socket}
pg1-user=chessism_user
''')
        self.config.chmod(0o600)

    async def command(self, phase, *args, capture=False):
        await self.publish(phase,phase_progress_percent=0)
        command=['pgbackrest',f'--config={self.config}','--stanza=refactor',*args]
        proc=await asyncio.create_subprocess_exec(*command,stdout=asyncio.subprocess.PIPE,
                                                stderr=asyncio.subprocess.STDOUT)
        lines=[]
        try:
            with (self.work/(phase+'.log')).open('w') as log:
                async for raw in proc.stdout:
                    line=raw.decode(errors='replace')
                    log.write(line)
                    if capture:
                        lines.append(line)
                    percent=re.findall(r'\b(\d{1,3})%',line)
                    if percent:
                        self.status['phase_progress_percent']=max(self.status.get('phase_progress_percent',0),
                                                                 min(99,int(percent[-1])))
            result=await proc.wait()
            if result:
                raise RuntimeError(f'Private pgBackRest {phase} failed; see its local log')
        except BaseException:
            if proc.returncode is None:
                proc.terminate()
                try:
                    await asyncio.wait_for(proc.wait(),timeout=30)
                except asyncio.TimeoutError:
                    proc.kill()
                    await proc.wait()
            raise
        self.status['phase_progress_percent']=100
        return ''.join(lines)

    async def run(self):
        if (self.source/'postmaster.pid').exists():
            raise RuntimeError('Source private PostgreSQL must be stopped for the offline backup')
        await self.command('creating_private_backup_stanza','--no-online','stanza-create')
        await self.command('backing_up_private_candidate','--no-online','--type=full','backup')
        info=json.loads(await self.command('reading_private_backup_manifest','--log-level-console=off','--output=json','info',capture=True))
        backup=info[0]['backup'][-1]
        label=backup['label']
        self.status.update(private_backup_id=label,private_backup_bytes=backup.get('info',{}).get('repository',{}).get('size'))
        await self.command('verifying_private_backup','verify')
        await self.command('restoring_private_backup',f'--set={label}',f'--pg1-path={self.restored}',
                           '--archive-mode=off','restore')
        return {'backup_id':label,'repository':str(self.work/'repository'),
                'restored_pgdata':str(self.restored),'backup_engine':'pgBackRest','mode':'offline',
                'limitations':['This tests an offline physical round-trip, not live WAL archiving or the backup UI job.']}
