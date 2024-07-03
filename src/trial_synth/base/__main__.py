from .config import BaseConfig

class Config(BaseConfig):
    def __post_init__(self):
        self.registry='who'
        super().__post_init__()

def main():
    config = Config()
    print(config.unprocessed_file_path)
    print(config.fields)


if __name__ == '__main__':
    main()