id := "com.github.ibanks42.opendeck-m18.sdPlugin"
cross_image := "ghcr.io/rust-cross/cargo-zigbuild@sha256:d8313491ec5798de0633fdc1c5753761bff79967bea69076020dc78121b2cca8"
windows_image := "rust@sha256:bf5a9aa29062a6cb03c49bd59a46eb55e3cc770caf598a221a7866e500be3082"

release: bump package tag

package: build-linux build-linux-arm64 build-mac build-win collect zip

bump next=`git cliff --bumped-version | tr -d "v"`:
    git diff --cached --exit-code

    echo "We will bump version to {{next}}, press any key"
    read ans

    sed -i 's/"Version": ".*"/"Version": "{{next}}"/g' manifest.json
    sed -i 's/^version = ".*"$/version = "{{next}}"/g' Cargo.toml

tag next=`git cliff --bumped-version`:
    echo "Generating changelog"
    git cliff -o CHANGELOG.md --tag {{next}}

    echo "We will now commit the changes, please review before pressing any key"
    read ans

    git add .
    git commit -m "chore(release): {{next}}"
    git tag "{{next}}"

build-linux:
    cargo build --release --locked --target x86_64-unknown-linux-gnu --target-dir target/plugin-linux

build-linux-arm64:
    docker run --rm -v $(pwd):/io -w /io {{cross_image}} cargo zigbuild --release --locked --target aarch64-unknown-linux-gnu --target-dir target/plugin-linux-arm64

build-mac:
    docker run --rm -v $(pwd):/io -w /io {{cross_image}} cargo zigbuild --release --locked --target universal2-apple-darwin --target-dir target/plugin-mac

build-win:
    docker run --rm -v $(pwd):/io -w /io {{windows_image}} sh -c "apt-get update && apt-get install -y gcc-mingw-w64-x86-64 && rustup target add x86_64-pc-windows-gnu && cargo build --release --locked --target x86_64-pc-windows-gnu --target-dir target/plugin-win"

clean:
    sudo rm -rf target/

collect:
    rm -rf build
    mkdir -p build/{{id}}
    cp -r assets build/{{id}}
    cp -r property_inspector build/{{id}}
    cp manifest.json build/{{id}}
    cp target/plugin-linux/x86_64-unknown-linux-gnu/release/opendeck-m18 build/{{id}}/opendeck-m18-linux
    cp target/plugin-linux-arm64/aarch64-unknown-linux-gnu/release/opendeck-m18 build/{{id}}/opendeck-m18-linux-aarch64
    cp target/plugin-mac/universal2-apple-darwin/release/opendeck-m18 build/{{id}}/opendeck-m18-mac
    cp target/plugin-win/x86_64-pc-windows-gnu/release/opendeck-m18.exe build/{{id}}/opendeck-m18-win.exe

[working-directory: "build"]
zip:
    zip -r opendeck-m18.plugin.zip {{id}}/
