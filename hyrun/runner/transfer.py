from pathlib import Path
from socket import gethostname


class FileTransferManager:
    """File transfer manager."""

    def get_remote_folder(self, jobs) -> str:
        """Get remote folder."""
        remote_folders = [getattr(t, 'submit_dir_remote', None)
                          or getattr(t, 'work_dir_remote', None)
                          for j in jobs.values()
                          for t in j['job'].tasks
                          if getattr(t, 'submit_dir_remote', None)
                          or getattr(t, 'work_dir_remote', None)]
        if len(list(set(remote_folders))) > 1:
            raise ValueError('All tasks in a job must have the same ' +
                             'remote folder')
        return str(remote_folders[0]) if len(remote_folders) == 1 else ''

    def transfer_from_cluster(self,
                              jobs=None,
                              scheduler=None,
                              connection=None,
                              **kwargs):
        """Transfer files from cluster."""
        transfer_all = kwargs.get('transfer_all', True)
        output_files = ['output_file', 'stdout_file', 'stderr_file']
        dfiles_to_transfer = {}
        for j in jobs.values():
            # finished = all(self.check_finished_single(t)
            #                for t in j['job'].tasks)
            # if not finished:
            #     self.logger.warning(f'Job {j["job"].id} not finished, not ' +
            #                         'transferring files')
            #     continue
            for t, o in zip(j['job'].tasks, j['job'].outputs):
                wdir_local = str(getattr(t, 'work_dir_local'))
                if wdir_local not in dfiles_to_transfer:
                    dfiles_to_transfer[wdir_local] = []

                if transfer_all:
                    for f in output_files:
                        output_file = getattr(o, f, None)
                        remote_dir = Path(output_file['path']).parent
                        alls = str(Path(remote_dir) / '*')
                        dfiles_to_transfer[wdir_local].append(
                            {'path': alls,
                             'host': getattr(connection,
                                             'host',
                                             gethostname())})
                        continue

                for f in output_files:
                    remote_file = getattr(o, f, None)
                    if remote_file:
                        dfiles_to_transfer[wdir_local].append(
                            remote_file)

        for wdir_local, files in dfiles_to_transfer.items():
            transfer = scheduler.transfer_files(  # type: ignore
                files_to_transfer=files,
                connection=connection,
                folder=wdir_local)
            log_method = (self.logger.info
                          if getattr(transfer, 'ok', True)
                          else self.logger.error)
            log_method(getattr(transfer, 'stdout'
                               if getattr(transfer, 'ok', True)
                               else 'stderr', ''))

    def transfer_to_cluster(self,
                            jobs=None,
                            scheduler=None,
                            connection=None):
        """Transfer files to cluster."""
        files_to_transfer = [j['job'].job_script for j in jobs.values()]

        files_to_transfer.extend([f for j in jobs.values()
                                  for t in j['job'].tasks
                                  for f in t.files_to_write])

        remote_folder = self.get_remote_folder(jobs)

        loc = f'{getattr(connection, "host", gethostname())}'
        loc += f':{remote_folder}' if remote_folder else ''
        self.logger.debug(f'Files to transfer: {files_to_transfer} to {loc}')

        transfer = scheduler.transfer_files(  # type: ignore
            files_to_transfer=files_to_transfer,
            connection=connection,
            folder=remote_folder)
        log_method = (self.logger.info
                      if getattr(transfer, 'ok', True)
                      else self.logger.error)
        log_method(getattr(transfer, 'stdout'
                           if getattr(transfer, 'ok', True) else 'stderr', ''))
