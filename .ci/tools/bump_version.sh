# Set version
PYSDK_PATH=./py-sdk
export VERSION=${GITHUB_REF_NAME#"py-sdk/v"}
ls 
sed -i -E "s/^(version =.*)/version = \"$VERSION\"/g" $PYSDK_PATH/pyproject.toml
echo -e "Getting tag to publish:\n    $(cat $PYSDK_PATH/pyproject.toml | grep "version =")"

