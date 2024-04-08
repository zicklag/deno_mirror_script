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
  .name("s3-sync")
  .version("0.1.0")
  .description("S3 sync command")
  .option(
    "-E, --env-auth",
    "Use environment variables for auth, instead of the ~/.aws/credentials file."
  )
  .arguments("<bucket:string>")
  .action(async (opts, bucket) => {
    let accessKey;
    let secretKey;

    if (opts.envAuth) {
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
        `Attempted to read auth from ${
          opts.envAuth
            ? "environment variables: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY."
            : "~/.aws/credentials default profile."
        }`
      );
      Deno.exit(1);
    }

    const s3client = new S3Client([
      {
        credentials: {
          accessKeyId: accessKey,
          secretAccessKey: secretKey,
        },
        bucketEndpoint: "s3.us-east-1.amazonaws.com",
        region: "us-east-1",
      },
    ]);

    const listObjs = async () => {
      console.log("Listing objects...");
      const command = new ListObjectsV2Command({
        Bucket: bucket,
      });
      let objs: ListObjectsV2CommandOutput["Contents"] = [];

      try {
        let isTruncated = true;
        while (isTruncated) {
          const { Contents, IsTruncated, NextContinuationToken } =
            await s3client.send(command);
          console.log(
            `    Received batch of ${(Contents || []).length} objects.`
          );
          // Note: we check for a non-null size to filter out directories.
          objs = [...objs, ...(Contents || []).filter((x) => !!x.Size)];
          isTruncated = IsTruncated || false;
          if (isTruncated) {
            console.log(`    More objects exist, fetching next batch.`);
          }
          command.input.ContinuationToken = NextContinuationToken;
        }
      } catch (err) {
        console.error(err);
      }

      console.log("List objects complete.");
      return objs;
    };

    const objs = await listObjs();

    console.log("Fetching object checksums...");
    let sumResults = await Promise.all(
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
          console.log(`    ${obj.Key}: ${head.ChecksumSHA256}`);
          return resp;
        })()
      )
    );
    const sums: { key: string; sum: string }[] = sumResults.filter(
      (x) => !!x
    ) as any;

    console.log("Fetch object sums complete.");
  })
  .parse(Deno.args);
