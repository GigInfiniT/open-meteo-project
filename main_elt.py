from src.pipeline.weather_elt_pipeline import ELTPipeline


def main():

    pipeline = ELTPipeline()

    pipeline.run()


if __name__ == "__main__":
    main()
