import re
from sys import executable as python_ex
from typing import List, Optional, Union


class GenCmd:
    """Generate command for running a list of commands."""

    def sanitize_cmd(self,
                     cmd: Union[str, List[str]],
                     delimiters: Optional[List[str]] = None
                     ) -> List[str]:
        """Sanitize command."""
        if delimiters is None:
            delimiters = [';', '|', '&&']
        if not cmd:
            return []
        if isinstance(cmd, list):
            cmd = ' '.join(cmd)
        elif not isinstance(cmd, str):
            raise TypeError('command must be a string')

        pattern = '|'.join(map(re.escape, delimiters))  # type: ignore
        parts = re.split(f'({pattern})', cmd)
        # parts = [part.strip() for part in parts if part.strip()]
        cmd = [
            subpart
            for part in parts
            for subpart in (part.split() if part not in delimiters else [part])
        ]

        if any([c in cmd[0] for c in delimiters]):
            cmd = cmd[1:]
        if not any([c in cmd[-1] for c in delimiters]):
            cmd.append(';')
        return cmd

    def split_by_delimiters(self,
                            commands: List[str], delimiters: List[str]
                            ) -> List[List[str]]:
        """Split a list of commands by delimiters into different lists."""
        result = []
        current_list = []

        for item in commands:
            current_list.append(item)
            if item in delimiters:
                result.append(current_list)
                current_list = []

        if current_list:
            result.append(current_list)

        return result

    def get_final_idx(self,
                      cmds: List[List[str]],
                      post_cmd: List[str],
                      running_list: List[str]
                      ) -> int:
        """Get index of final result based on post_cmd and running_list."""
        if not post_cmd:
            return -1  # return the last result

        first_part_of_post_cmd = self.split_by_delimiters(
            post_cmd, ['|', '&&']
        )[0]

        for i, c in enumerate(cmds):
            if c == first_part_of_post_cmd:
                break

        if running_list[-1] in [';', '&&']:
            return i - 1
        else:
            # main result is piped to post_cmd
            for j in range(i + 1, len(cmds)):
                if cmds[j][-1] in [';', '&&']:
                    return j

        return -1

    def gen_cmd(self,
                pre_cmd: Optional[Union[str, List[str]]] = None,
                main_cmd: Optional[Union[str, List[str]]] = None,
                post_cmd: Optional[Union[str, List[str]]] = None
                ) -> List[List[str]]:
        """Generate command."""
        pre_cmd = self.sanitize_cmd(pre_cmd)
        post_cmd = self.sanitize_cmd(post_cmd)
        main_cmd = self.sanitize_cmd(main_cmd)
        main_cmd[-1] = (
            '&&' if main_cmd[-1] == ';' else main_cmd[-1]
        )

        cmd = pre_cmd + main_cmd + post_cmd

        for c in cmd:
            if 'python' in c:
                c = c.replace('python', python_ex)

        return self.split_by_delimiters(cmd, [';', '|', '&&'])
