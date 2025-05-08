import sys, os

current_dir = os.path.dirname(__file__)
parent_dir =  os.path.abspath(os.path.join(current_dir, os.pardir))
grand_parent_dir = os.path.abspath(os.path.join(parent_dir, os.pardir))
src_dir = os.path.abspath(os.path.join(grand_parent_dir, os.pardir))  

sys.path.append(parent_dir)
sys.path.append(grand_parent_dir)
sys.path.append(src_dir)