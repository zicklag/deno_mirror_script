// deno-lint-ignore-file no-explicit-any

import * as ini from "https://deno.land/x/ini@v2.1.0/ini.ts";
// import { S3Client } from "https://deno.land/x/s3_lite_client@0.7.0/mod.ts";
import { join } from "https://deno.land/std@0.221.0/path/mod.ts";
import { sha256 } from "https://denopkg.com/chiefbiiko/sha256@v1.0.0/mod.ts";

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
    "Only check items in this prefix of the S3 bucket."
  )
  .arguments("<bucket:string> <dir:string>")
  .action(async ({ prefix }, bucket, dir) => {
    let accessKey;
    let secretKey;

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

    const s3client = new S3Client([
      {
        credentials: {
          accessKeyId: accessKey,
          secretAccessKey: secretKey,
        },
      },
    ]);

    const listObjs = async () => {
      console.log("Listing objects...");
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
          console.log(`    Received batch of ${files.length} objects.`);
          objs = [...objs, ...files];
          isTruncated = IsTruncated || false;
          if (isTruncated) {
            console.log(`    More objects exist, fetching next batch.`);
          }
          command.input.ContinuationToken = NextContinuationToken;
        }
      } catch (err) {
        console.error(err);
      }

      console.log(`Listing objects complete: ${objs.length} objects total.`);
      return objs;
    };

    const objs = await listObjs();

    console.log("Fetching object checksums...");
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
          console.log(`    ${head.ChecksumSHA256}: ${obj.Key}`);
          return resp;
        })()
      )
    );
    const sums: { key: string; sum: string }[] = sumResults.filter(
      (x) => !!x
    ) as any;

    console.log("Fetch object sums complete.");

    console.log("Calculating and comparing checksums...");
    let matches = 0;
    let missing = 0;
    let mismatches = 0;
    await Promise.all(
      sums.map(({ key, sum }) =>
        (async () => {
          const path = join(dir, key);

          // Make sure the file exists
          try {
            await Deno.stat(path);
          } catch (_) {
            console.error(`File on S3 doesn't exist locally: ${key}`);
            missing += 1;
            return;
          }

          // Read file and compute the sum
          const file = await Deno.readFile(path);
          const localSum = sha256(file, undefined, "base64");
          if (localSum != sum) {
            console.error(`    Error: checksum mismatch: ${key}`);
            console.error(`           S3   : ${sum}`);
            console.error(`           Local: ${localSum}`);
            mismatches += 1;
          } else {
            console.log(`    Match: ${key}`);
            matches += 1;
          }
        })()
      )
    );
    console.log(`Done comparing sums.`);
    console.log(`    Total     : ${sums.length}`);
    console.log(`    Matches   : ${matches}`);
    console.log(`    Mismatches: ${mismatches}`);
    console.log(`    Missing   : ${missing}`);
  })
  .parse(Deno.args);
