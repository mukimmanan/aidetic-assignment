from utils import get_sparksession, connect_kafka, convert_to_columns, write_data, process_data


def main():
    kafka = connect_kafka(get_sparksession(), "localhost:9095,localhost:9093", "aidetic")
    kafka = convert_to_columns(kafka)
    kafka.printSchema()
    write_data(process_data(kafka))


if __name__ == '__main__':
    main()