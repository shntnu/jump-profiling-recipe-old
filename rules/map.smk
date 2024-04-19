rule average_precision_negcon:
    input:
        "outputs/{prefix}/{pipeline}.parquet",
    output:
        "outputs/{prefix}/metrics/{pipeline}_ap_negcon.parquet",
    params:
        plate_types=config["plate_types"],
        negcon_codes=config["values_norm"],
    run:
        pp.metrics.average_precision_negcon(*input, *output, **params)


rule average_precision_nonrep:
    input:
        "outputs/{prefix}/{pipeline}.parquet",
    output:
        "outputs/{prefix}/metrics/{pipeline}_ap_nonrep.parquet",
    params:
        plate_types=config.get("plate_types", None),
        negcon_codes=config.get("values_norm", None),
        pos_sameby=config.get("pos_sameby", "Metadata_JCP2022"),
        pos_diffby=config.get("pos_diffby", None),
        neg_sameby=config.get("neg_sameby", "Metadata_Plate"),
        neg_diffby=config.get("neg_diffby", "Metadata_JCP2022"),
    run:
        pp.metrics.average_precision_nonrep(*input, *output, **params)


rule mean_average_precision:
    input:
        "outputs/{prefix}/metrics/{pipeline}_ap_{reftype}.parquet",
    output:
        "outputs/{prefix}/metrics/{pipeline}_map_{reftype}.parquet",
    run:
        pp.metrics.mean_average_precision(*input, *output)
