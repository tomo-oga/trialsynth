from .process import Processor
import cProfile
import pstats

def main():
    processor = Processor()

    processor.load_data()
    processor.set_nodes_and_edges()

if __name__ == '__main__':
        main()
