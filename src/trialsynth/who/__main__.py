from .process import Processor
import cProfile
import pstats

def main():
    processor = Processor()

    processor.run()

if __name__ == '__main__':
        main()
