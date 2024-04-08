// deno-lint-ignore-file no-explicit-any

import * as ini from "https://deno.land/x/ini@v2.1.0/ini.ts";
import { join } from "https://deno.land/std@0.221.0/path/mod.ts";
import { sha256 } from "https://denopkg.com/chiefbiiko/sha256@v1.0.0/mod.ts";
import { walk } from "https://deno.land/std@0.221.0/fs/walk.ts";

import {
  S3Client,
  HeadObjectCommand,
  ListObjectsV2Command,
  ListObjectsV2CommandOutput,
} from "npm:@aws-sdk/client-s3";

import { Command } from "https://deno.land/x/cliffy@v1.0.0-rc.3/command/mod.ts";
import { assert } from "https://deno.land/std@0.221.0/assert/assert.ts";

await new Command()
  .name("s3-checksum-compare")
  .version("0.1.0")
  .description(
    "Compares the checksums of an S3 bucket with the files in a local dir to make sure they are the same."
  )
  .option(
    "-p, --prefix <prefix>",
    "Only check items in this prefix of the S3 bucket and directory."
  )
  .option("-E, --endpoint <endpoint>", "Set the S3 endpoint to use.")
  .option("-v, --verbose", "Log info as well as well warnings.")
  .option("-R, --region <region>", "The AWS region the bucket is in.")
  .option("-j, --json", "Output the JSON details to stdout when finished.")
  .arguments("<bucket:string> <dir:string>")
  .action(async ({ prefix, verbose, json, region, endpoint }, bucket, dir) => {
    const dirWithPrefix = prefix ? join(dir, prefix) : dir;
    let accessKey;
    let secretKey;
    region = region || "us-east-1";

    if (Deno.env.get("AWS_ACCESS_KEY_ID")) {
      accessKey = Deno.env.get("AWS_ACCESS_KEY_ID");
      secretKey = Deno.env.get("AWS_SECRET_ACCESS_KEY");
    } else {
      const credentials: {
        default: { aws_access_key_id: string; aws_secret_access_key: string };
      } = ini.decode(
        await Deno.readTextFile(
          join(Deno.env.get("HOME")!, await ".aws/credentials")
        )
      ) as any;
      accessKey = credentials.default.aws_access_key_id;
      secretKey = credentials.default.aws_secret_access_key;
    }

    if (!(accessKey && secretKey)) {
      console.error("Missing access key or secret key.");
      console.error(
        `Attempted to read auth from environment variables: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY. and then the ~/.aws/credentials default profile.`
      );
      Deno.exit(1);
    }

    const s3client = new S3Client({
      region,
      endpoint,
      followRegionRedirects: true,
      credentials: {
        accessKeyId: accessKey,
        secretAccessKey: secretKey,
      },
    } as any);

    const listObjs = async () => {
      console.error("Listing objects...");
      const command = new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: prefix,
      });
      let objs: ListObjectsV2CommandOutput["Contents"] = [];

      try {
        let isTruncated = true;
        while (isTruncated) {
          const { Contents, IsTruncated, NextContinuationToken } =
            await s3client.send(command);
          // Note: we check for a non-null size to filter out directories.
          const files = (Contents || []).filter((x) => !!x.Size);
          console.error(`    Received batch of ${files.length} objects.`);
          objs = [...objs, ...files];
          isTruncated = IsTruncated || false;
          if (isTruncated) {
            console.error(`    More objects exist, fetching next batch.`);
          }
          command.input.ContinuationToken = NextContinuationToken;
        }
      } catch (err) {
        console.error(err);
      }

      console.error(`Listing objects complete: ${objs.length} objects total.`);
      return objs;
    };

    const objs = await listObjs();
    const localWalk = await walk(dirWithPrefix, { includeDirs: false });
    const localFiles: string[] = [];
    for await (const entry of localWalk) {
      const path = entry.path.slice(dir.length);
      localFiles.push(path);
    }

    const matches: string[] = [];
    const mismatches: string[] = [];
    const missing_locally: string[] = [];
    const missing_on_s3: string[] = [];

    console.error(`Scanning local files...`);
    for (const path of localFiles) {
      if (!objs.find((x) => x.Key == path)) {
        console.error(
          `    Error: file found locally that is not on S3: ${path}`
        );
        missing_on_s3.push(path);
      }
    }
    console.log(`Done scanning local files: ${localFiles.length} files total.`);

    console.error("Fetching object checksums...");
    const sumResults = await Promise.all(
      objs.map((obj) =>
        (async () => {
          assert(obj.Key, "Missing object key");
          const head = await s3client.send(
            new HeadObjectCommand({
              Key: obj.Key,
              Bucket: bucket,
              ChecksumMode: "ENABLED",
            })
          );
          if (!head.ChecksumSHA256) {
            console.warn(
              `Skipping object with missing Sha256 Checksum: ${head.ChecksumSHA256}`
            );
            return null;
          }
          const resp = { key: obj.Key, sum: head.ChecksumSHA256 };
          if (verbose) {
            console.error(`    ${head.ChecksumSHA256}: ${obj.Key}`);
          }
          return resp;
        })()
      )
    );
    const sums: { key: string; sum: string }[] = sumResults.filter(
      (x) => !!x
    ) as any;

    console.error("Fetch object sums complete.");

    console.error("Calculating and comparing checksums...");
    await Promise.all(
      sums.map(({ key, sum }) =>
        (async () => {
          const path = join(dir, key);

          // Make sure the file exists
          try {
            await Deno.stat(path);
          } catch (_) {
            console.error(`    File on S3 doesn't exist locally: ${key}`);
            missing_locally.push(key);
            return;
          }

          // Read file and compute the sum
          const file = await Deno.readFile(path);
          const localSum = sha256(file, undefined, "base64");
          if (localSum != sum) {
            console.error(`    Error: checksum mismatch: ${key}`);
            console.error(`           S3   : ${sum}`);
            console.error(`           Local: ${localSum}`);
            mismatches.push(key);
          } else {
            if (verbose) {
              console.error(`    Match: ${key}`);
            }
            matches.push(key);
          }
        })()
      )
    );
    console.error(`Done comparing sums.`);
    console.error(`    Total     : ${sums.length}`);
    console.error(`    Matches   : ${matches.length}`);
    console.error(`    Mismatches: ${mismatches.length}`);
    console.error(`    Missing Locally: ${missing_locally.length}`);
    console.error(`    Missing on S3  : ${missing_on_s3.length}`);

    if (json) {
      console.log(
        JSON.stringify(
          { matches, missing_locally, missing_on_s3, mismatches },
          null,
          2
        )
      );
    }
  })
  .parse(Deno.args);
