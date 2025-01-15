# import unittest

# from hyrun.runner.gen_cmd import GenCmd


# class TestGenCmd(unittest.TestCase):

#     def setUp(self):
#         self.gen_cmd = GenCmd()

#     def test_sanitize_cmd(self):
#         self.assertEqual(self.gen_cmd.sanitize_cmd('echo hello'),
#                          ['echo', 'hello', ';'])
#         self.assertEqual(self.gen_cmd.sanitize_cmd(['echo', 'hello']),
#                          ['echo', 'hello', ';'])
#         self.assertEqual(self.gen_cmd.sanitize_cmd('; echo hello && ls'),
#                          ['echo', 'hello', '&&', 'ls', ';'])
#         self.assertEqual(self.gen_cmd.sanitize_cmd('echo hello; ls'),
#                          ['echo', 'hello', ';', 'ls', ';'])
#         self.assertEqual(self.gen_cmd.sanitize_cmd('echo hello | grep h'),
#                          ['echo', 'hello', '|', 'grep', 'h', ';'])

#     def test_split_by_delimiters(self):
#         commands = ['echo', 'hello', '&&', 'ls', ';']
#         delimiters = [';', '&&']
#         expected = [['echo', 'hello', '&&'], ['ls', ';']]
#         self.assertEqual(self.gen_cmd.split_by_delimiters(commands,
#                                                           delimiters),
#                                                           expected)

#     def test_get_final_idx(self):
#         cmds = [['echo', 'hello', '&&'], ['ls', ';']]
#         post_cmd = ['ls', ';']
#         running_list = ['echo', 'hello', '&&']
#         self.assertEqual(self.gen_cmd.get_final_idx(cmds, post_cmd,
#                                                     running_list), 0)

#     def test_gen_cmd(self):
#         pre_cmd = 'echo pre'
#         main_cmd = 'echo main'
#         post_cmd = 'echo post'
#         expected = [['echo', 'pre', ';'],
#                     ['echo', 'main', '&&'],
#                     ['echo', 'post', ';']]
#         self.assertEqual(self.gen_cmd.gen_cmd(pre_cmd, main_cmd, post_cmd),
#                          expected)

#         pre_cmd = ['echo', 'pre']
#         main_cmd = ['echo', 'main']
#         post_cmd = ['echo', 'post']
#         expected = [['echo', 'pre', ';'],
#                     ['echo', 'main', '&&'],
#                     ['echo', 'post', ';']]
#         self.assertEqual(self.gen_cmd.gen_cmd(pre_cmd, main_cmd, post_cmd),
#                          expected)

# if __name__ == '__main__':
#     unittest.main()
